package ai.chronon.flink

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Constants
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.DataType
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.SchemaRegistrySchemaProvider.RegistryHostKey
import ai.chronon.flink.types.AvroCodecOutput
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.flink.types.WriteResponse
import ai.chronon.flink.validation.ValidationFlinkJob
import ai.chronon.flink.window.AlwaysFireOnElementTrigger
import ai.chronon.flink.window.FlinkRowAggProcessFunction
import ai.chronon.flink.window.FlinkRowAggregationFunction
import ai.chronon.flink.window.KeySelectorBuilder
import ai.chronon.online.Api
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.TopicInfo
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.StateBackendOptions
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.OutputTag
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Serialization
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.Seq

/** Flink job that processes a single streaming GroupBy and writes out the results to the KV store.
  *
  * There are two versions of the job, tiled and untiled. The untiled version writes out raw events while the tiled
  * version writes out pre-aggregates. See the `runGroupByJob` and `runTiledGroupByJob` methods for more details.
  *
  * @param eventSrc - Provider of a Flink Datastream[T] for the given topic and groupBy
  * @param sinkFn - Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed - The GroupBy we are working with
  * @param parallelism - Parallelism to use for the Flink job
  */
class FlinkJob(eventSrc: ProjectedKafkaFlinkSource,
                  inputSchema: Seq[(String, DataType)],
                  sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                  groupByServingInfoParsed: GroupByServingInfoParsed,
                  parallelism: Int) {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink job. groupByName=${groupByName}")

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid groupBy: $groupByName. No streaming source"
    )
  }

  // The source of our Flink application is a  topic
  val topic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  /** The "tiled" version of the Flink app.
    *
    * The operators are structured as follows:
    *  1.  source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *  2. Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input
    *      data
    *  3. Window/tiling - This window aggregates incoming events, keeps track of the IRs, and sends them forward so
    *      they are written out to the KV store
    *  4. Avro conversion - Finishes converting the output of the window (the IRs) to a form that can be written out
    *      to the KV store (PutRequest object)
    *  5. KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  The window causes a split in the Flink DAG, so there are two nodes, (1+2) and (3+4+5).
    */
  def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. " +
        "Tiling is enabled.")

    val tilingWindowSizeInMillis: Long =
      ResolutionUtils.getSmallestWindowResolutionInMillis(groupByServingInfoParsed.groupBy)

    // we expect parallelism on the source stream to be set by the source provider
    val sourceSparkProjectedStream: DataStream[Map[String, Any]] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDSAndWatermarks: DataStream[Map[String, Any]] = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis))
      .asInstanceOf[WindowAssigner[Map[String, Any], TimeWindow]]

    // An alternative to AlwaysFireOnElementTrigger can be used: BufferedProcessingTimeTrigger.
    // The latter will buffer writes so they happen at most every X milliseconds per GroupBy & key.
    val trigger = new AlwaysFireOnElementTrigger()

    // We use Flink "Side Outputs" to track any late events that aren't computed.
    val tilingLateEventsTag = new OutputTag[Map[String, Any]]("tiling-late-events") {}

    // The tiling operator works the following way:
    // 1. Input: Spark expression eval (previous operator)
    // 2. Key by the entity key(s) defined in the groupby
    // 3. Window by a tumbling window
    // 4. Use our custom trigger that will "FIRE" on every element
    // 5. the AggregationFunction merges each incoming element with the current IRs which are kept in state
    //    - Each time a "FIRE" is triggered (i.e. on every event), getResult() is called and the current IRs are emitted
    // 6. A process window function does additional processing each time the AggregationFunction emits results
    //    - The only purpose of this window function is to mark tiles as closed so we can do client-side caching in SFS
    // 7. Output: TimestampedTile, containing the current IRs (Avro encoded) and the timestamp of the current element

    val tilingDS: SingleOutputStreamOperator[TimestampedTile] =
      sparkExprEvalDSAndWatermarks
        .keyBy(KeySelectorBuilder.build(groupByServingInfoParsed.groupBy))
        .window(window)
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          // See Flink's "ProcessWindowFunction with Incremental Aggregation"
          new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, inputSchema),
          new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, inputSchema)
        )
        .uid(s"tiling-01-$groupByName")
        .name(s"Tiling for $groupByName")
        .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Track late events
    tilingDS
      .getSideOutput(tilingLateEventsTag)
      .flatMap(new LateEventCounter(groupByName))
      .uid(s"tiling-side-output-01-$groupByName")
      .name(s"Tiling Side Output Late Data for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val putRecordDS: DataStream[AvroCodecOutput] = tilingDS
      .flatMap(TiledAvroCodecFn(groupByServingInfoParsed, tilingWindowSizeInMillis))
      .uid(s"avro-conversion-01-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      groupByName
    )
  }
}

object FlinkJob {
  // we set an explicit max parallelism to ensure if we do make parallelism setting updates, there's still room
  // to restore the job from prior state. Number chosen does have perf ramifications if too high (can impact rocksdb perf)
  // so we've chosen one that should allow us to scale to jobs in the 10K-50K events / s range.
  val MaxParallelism: Int = 1260 // highly composite number

  // We choose to checkpoint frequently to ensure the incremental checkpoints are small in size
  // as well as ensuring the catch-up backlog is fairly small in case of failures
  val CheckPointInterval: FiniteDuration = 10.seconds

  // We set a more lenient checkpoint timeout to guard against large backlog / catchup scenarios where checkpoints
  // might be slow and a tight timeout will set us on a snowball restart loop
  val CheckpointTimeout: FiniteDuration = 5.minutes

  // We use incremental checkpoints and we cap how many we keep around
  val MaxRetainedCheckpoints: Int = 10

  // how many consecutive checkpoint failures can we tolerate - default is 0, we choose a more lenient value
  // to allow us a few tries before we give up
  val TolerableCheckpointFailures: Int = 5

  // Keep windows open for a bit longer before closing to ensure we don't lose data due to late arrivals (needed in case of
  // tiling implementation)
  val AllowedOutOfOrderness: Duration = Duration.ofMinutes(5)

  // Set an idleness timeout to keep time moving in case of very low traffic event streams as well as late events during
  // large backlog catchups
  val IdlenessTimeout: Duration = Duration.ofSeconds(30)

  // We wire up the watermark strategy post the spark expr eval to be able to leverage the user's timestamp column (which is
  // ETLed to Contants.TimeColumn) as the event timestamp and watermark
  val watermarkStrategy: WatermarkStrategy[Map[String, Any]] = WatermarkStrategy
    .forBoundedOutOfOrderness[Map[String, Any]](AllowedOutOfOrderness)
    .withIdleness(IdlenessTimeout)
    .withTimestampAssigner(new SerializableTimestampAssigner[Map[String, Any]] {
      override def extractTimestamp(element: Map[String, Any], recordTimestamp: Long): Long = {
        element.get(Constants.TimeColumn).map(_.asInstanceOf[Long]).getOrElse(recordTimestamp)
      }
    })

  // Pull in the Serialization trait to sidestep: https://github.com/scallop/scallop/issues/137
  class JobArgs(args: Seq[String]) extends ScallopConf(args) with Serialization {
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")
    val groupbyName: ScallopOption[String] =
      opt[String](required = true, descr = "The name of the groupBy to process")
    val mockSource: ScallopOption[Boolean] =
      opt[Boolean](required = false, descr = "Use a mocked data source instead of a real source", default = Some(false))
    // Kafka config is optional as we can support other sources in the future
    val kafkaBootstrap: ScallopOption[String] =
      opt[String](required = false, descr = "Kafka bootstrap server in host:port format")
    // Run in validate mode - We read rows using Kafka and run them through Spark Df and compare against CatalystUtil output
    val validate: ScallopOption[Boolean] =
      opt[Boolean](required = false, descr = "Run in validate mode", default = Some(false))
    // Number of rows to use for validation
    val validateRows: ScallopOption[Int] =
      opt[Int](required = false, descr = "Number of rows to use for validation", default = Some(10000))

    val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API / KV Store")

    verify()
  }

  def main(args: Array[String]): Unit = {
    val jobArgs = new JobArgs(args)
    val groupByName = jobArgs.groupbyName()
    val onlineClassName = jobArgs.onlineClass()
    val props = jobArgs.apiProps.map(identity)
    val kafkaBootstrap = jobArgs.kafkaBootstrap.toOption
    val validateMode = jobArgs.validate()
    val validateRows = jobArgs.validateRows()

    val api = buildApi(onlineClassName, props)
    val metadataStore = new MetadataStore(FetchContext(api.genKvStore, MetadataDataset))

    if (validateMode) {
      val validationResults = ValidationFlinkJob.run(metadataStore, kafkaBootstrap, groupByName, validateRows)
      if (validationResults.map(_.totalMismatches).sum > 0) {
        val validationSummary = s"Total records: ${validationResults.map(_.totalRecords).sum}, " +
          s"Total matches: ${validationResults.map(_.totalMatches).sum}, " +
          s"Total mismatches: ${validationResults.map(_.totalMismatches).sum}"
        throw new IllegalStateException(
          s"Spark DF vs Catalyst util validation failed. Validation summary: $validationSummary")
      }
    }

    val maybeServingInfo = metadataStore.getGroupByServingInfo(groupByName)
    val flinkJob =
      maybeServingInfo
        .map { servingInfo =>
          val topicUri = servingInfo.groupBy.streamingSource.get.topic
          val topicInfo = TopicInfo.parse(topicUri)

          val schemaProvider =
            topicInfo.params.get(RegistryHostKey) match {
              case Some(_) => new ProjectedSchemaRegistrySchemaProvider(topicInfo.params)
              case None =>
                throw new IllegalArgumentException(
                  s"We only support schema registry based schema lookups. Missing $RegistryHostKey in topic config")
            }

          val deserializationSchema = schemaProvider.buildProjectedSourceDeserializer(servingInfo.groupBy)
          val source =
            topicInfo.messageBus match {
              case "kafka" =>
                new ProjectedKafkaFlinkSource(kafkaBootstrap, deserializationSchema, topicInfo)
              case _ =>
                throw new IllegalArgumentException(s"Unsupported message bus: ${topicInfo.messageBus}")
            }

          new FlinkJob(
            eventSrc = source,
            deserializationSchema.projectedSchema,
            sinkFn = new AsyncKVStoreWriter(api, servingInfo.groupBy.metaData.name),
            groupByServingInfoParsed = servingInfo,
            parallelism = source.parallelism
          )
        }
        .recover { case e: Exception =>
          throw new IllegalArgumentException(s"Unable to lookup serving info for GroupBy: '$groupByName'", e)
        }
        .get

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(CheckPointInterval.toMillis, CheckpointingMode.AT_LEAST_ONCE)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setMinPauseBetweenCheckpoints(CheckPointInterval.toMillis)
    checkpointConfig.setCheckpointTimeout(CheckpointTimeout.toMillis)
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    checkpointConfig.setTolerableCheckpointFailureNumber(TolerableCheckpointFailures)
    // for now we retain our checkpoints even when we can cancel to allow us to resume from where we left off
    // post orchestrator, we will trigger savepoints on deploys and we can switch to delete on cancel
    checkpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val config = new Configuration()

    config.set(StateBackendOptions.STATE_BACKEND, "rocksdb")
    config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true)
    config.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, MaxRetainedCheckpoints)

    env.setMaxParallelism(MaxParallelism)

    env.getConfig.disableAutoGeneratedUIDs() // we generate UIDs manually to ensure consistency across runs
    env.getConfig
      .enableForceKryo() // use kryo for complex types that Flink's default ser system doesn't support (e.g case classes)
    env.getConfig.enableGenericTypes() // more permissive type checks

    env.configure(config)

    val jobDatastream = flinkJob.runTiledGroupByJob(env)

    jobDatastream
      .addSink(new MetricsSink(flinkJob.groupByName))
      .uid(s"metrics-sink - ${flinkJob.groupByName}")
      .name(s"Metrics Sink for ${flinkJob.groupByName}")
      .setParallelism(jobDatastream.getParallelism)

    env.execute(s"${flinkJob.groupByName}")
  }

  private def buildApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader // Use Flink's classloader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props)
    onlineImpl.asInstanceOf[Api]
  }
}
