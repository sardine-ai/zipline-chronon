package ai.chronon.flink

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Constants
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.DataType
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.window.AlwaysFireOnElementTrigger
import ai.chronon.flink.window.FlinkRowAggProcessFunction
import ai.chronon.flink.window.FlinkRowAggregationFunction
import ai.chronon.flink.window.KeySelector
import ai.chronon.flink.window.TimestampedTile
import ai.chronon.online.Api
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.MetadataStore
import ai.chronon.online.SparkConversions
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.configuration.CheckpointingOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.StateBackendOptions
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.spark.sql.Encoder
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Serialization
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
  * Flink job that processes a single streaming GroupBy and writes out the results to the KV store.
  *
  * There are two versions of the job, tiled and untiled. The untiled version writes out raw events while the tiled
  * version writes out pre-aggregates. See the `runGroupByJob` and `runTiledGroupByJob` methods for more details.
  *
  * @param eventSrc - Provider of a Flink Datastream[T] for the given topic and groupBy
  * @param sinkFn - Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed - The GroupBy we are working with
  * @param encoder - Spark Encoder for the input data type
  * @param parallelism - Parallelism to use for the Flink job
  * @tparam T - The input data type
  */
class FlinkJob[T](eventSrc: FlinkSource[T],
                  sinkFn: RichAsyncFunction[PutRequest, WriteResponse],
                  groupByServingInfoParsed: GroupByServingInfoParsed,
                  encoder: Encoder[T],
                  parallelism: Int) {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink job. groupByName=${groupByName}")

  protected val exprEval: SparkExpressionEvalFn[T] =
    new SparkExpressionEvalFn[T](encoder, groupByServingInfoParsed.groupBy)

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid groupBy: $groupByName. No streaming source"
    )
  }

  // The source of our Flink application is a  topic
  val topic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  /**
    * The "untiled" version of the Flink app.
    *
    *  At a high level, the operators are structured as follows:
    *    source -> Spark expression eval -> Avro conversion -> KV store writer
    *    source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *   Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data
    *   Avro conversion - Converts the Spark expr eval output to a form that can be written out to the KV store
    *      (PutRequest object)
    *   KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  In this untiled version, there are no shuffles and thus this ends up being a single node in the Flink DAG
    *  (with the above 4 operators and parallelism as injected by the user).
    */
  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. " +
        "Tiling is disabled.")

    // we expect parallelism on the source stream to be set by the source provider
    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-$groupByName")
      .name(s"Spark expression eval for $groupByName")
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val sparkExprEvalDSWithWatermarks: DataStream[Map[String, Any]] = sparkExprEvalDS
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceStream.parallelism)

    val putRecordDS: DataStream[PutRequest] = sparkExprEvalDSWithWatermarks
      .flatMap(AvroCodecFn[T](groupByServingInfoParsed))
      .uid(s"avro-conversion-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceStream.parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      groupByName
    )
  }

  /**
    * The "tiled" version of the Flink app.
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

    val tilingWindowSizeInMillis: Option[Long] =
      ResolutionUtils.getSmallestWindowResolutionInMillis(groupByServingInfoParsed.groupBy)

    // we expect parallelism on the source stream to be set by the source provider
    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-$groupByName")
      .name(s"Spark expression eval for $groupByName")
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val sparkExprEvalDSAndWatermarks: DataStream[Map[String, Any]] = sparkExprEvalDS
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceStream.parallelism)

    val inputSchema: Seq[(String, DataType)] =
      exprEval.getOutputSchema.fields
        .map(field => (field.name, SparkConversions.toChrononType(field.name, field.dataType)))
        .toSeq

    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis.get))
      .asInstanceOf[WindowAssigner[Map[String, Any], TimeWindow]]

    // An alternative to AlwaysFireOnElementTrigger can be used: BufferedProcessingTimeTrigger.
    // The latter will buffer writes so they happen at most every X milliseconds per GroupBy & key.
    val trigger = new AlwaysFireOnElementTrigger()

    // We use Flink "Side Outputs" to track any late events that aren't computed.
    val tilingLateEventsTag = OutputTag[Map[String, Any]]("tiling-late-events")

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
    val tilingDS: DataStream[TimestampedTile] =
      sparkExprEvalDSAndWatermarks
        .keyBy(KeySelector.getKeySelectionFunction(groupByServingInfoParsed.groupBy))
        .window(window)
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          // See Flink's "ProcessWindowFunction with Incremental Aggregation"
          preAggregator = new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, inputSchema),
          windowFunction = new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, inputSchema)
        )
        .uid(s"tiling-01-$groupByName")
        .name(s"Tiling for $groupByName")
        .setParallelism(sourceStream.parallelism)

    // Track late events
    tilingDS
      .getSideOutput(tilingLateEventsTag)
      .flatMap(new LateEventCounter(groupByName))
      .uid(s"tiling-side-output-01-$groupByName")
      .name(s"Tiling Side Output Late Data for $groupByName")
      .setParallelism(sourceStream.parallelism)

    val putRecordDS: DataStream[PutRequest] = tilingDS
      .flatMap(new TiledAvroCodecFn[T](groupByServingInfoParsed))
      .uid(s"avro-conversion-01-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceStream.parallelism)

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
  val MaxParallelism = 1260 // highly composite number

  // We choose to checkpoint frequently to ensure the incremental checkpoints are small in size
  // as well as ensuring the catch-up backlog is fairly small in case of failures
  val CheckPointInterval: FiniteDuration = 10.seconds

  // We set a more lenient checkpoint timeout to guard against large backlog / catchup scenarios where checkpoints
  // might be slow and a tight timeout will set us on a snowball restart loop
  val CheckpointTimeout: FiniteDuration = 5.minutes

  // We use incremental checkpoints and we cap how many we keep around
  val MaxRetainedCheckpoints = 10

  // how many consecutive checkpoint failures can we tolerate - default is 0, we choose a more lenient value
  // to allow us a few tries before we give up
  val TolerableCheckpointFailures = 5

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

    val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API / KV Store")

    verify()
  }

  def main(args: Array[String]): Unit = {
    val jobArgs = new JobArgs(args)
    val groupByName = jobArgs.groupbyName()
    val onlineClassName = jobArgs.onlineClass()
    val props = jobArgs.apiProps.map(identity)
    val useMockedSource = jobArgs.mockSource()
    val kafkaBootstrap = jobArgs.kafkaBootstrap.toOption

    val api = buildApi(onlineClassName, props)
    val metadataStore = new MetadataStore(api.genKvStore, MetadataDataset, timeoutMillis = 10000)

    val flinkJob =
      if (useMockedSource) {
        // We will yank this conditional block when we wire up our real sources etc.
        TestFlinkJob.buildTestFlinkJob(api)
      } else {
        val maybeServingInfo = metadataStore.getGroupByServingInfo(groupByName)
        maybeServingInfo
          .map { servingInfo =>
            val topicUri = servingInfo.groupBy.streamingSource.get.topic
            val topicInfo = TopicInfo.parse(topicUri)

            val schemaProvider =
              topicInfo.params.get("registry_url") match {
                case Some(_) => new SchemaRegistrySchemaProvider(topicInfo.params)
                case None =>
                  throw new IllegalArgumentException(
                    "We only support schema registry based schema lookups. Missing registry_url in topic config")
              }

            val (encoder, deserializationSchema) = schemaProvider.buildEncoderAndDeserSchema(topicInfo)
            val source =
              topicInfo.messageBus match {
                case "kafka" =>
                  new KafkaFlinkSource(kafkaBootstrap, deserializationSchema, topicInfo)
                case _ =>
                  throw new IllegalArgumentException(s"Unsupported message bus: ${topicInfo.messageBus}")
              }

            new FlinkJob(
              eventSrc = source,
              sinkFn = new AsyncKVStoreWriter(api, servingInfo.groupBy.metaData.name),
              groupByServingInfoParsed = servingInfo,
              encoder = encoder,
              parallelism = source.parallelism
            )

          }
          .recover {
            case e: Exception =>
              throw new IllegalArgumentException(s"Unable to lookup serving info for GroupBy: '$groupByName'", e)
          }
          .get
      }

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

    val jobDatastream = flinkJob
      .runGroupByJob(env)

    jobDatastream
      .addSink(new MetricsSink(flinkJob.groupByName))
      .uid(s"metrics-sink - ${flinkJob.groupByName}")
      .name(s"Metrics Sink for ${flinkJob.groupByName}")
      .setParallelism(jobDatastream.parallelism)

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
