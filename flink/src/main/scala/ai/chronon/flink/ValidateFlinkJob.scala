package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.flink.SchemaRegistrySchemaProvider.RegistryHostKey
import ai.chronon.online.fetcher.MetadataStore
import ai.chronon.online.{GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.spark.sql.{Encoder, Row}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

case class ComparisonResult(recordId: String,
                            isMatch: Boolean,
                            catalystResult: Seq[Map[String, Any]],
                            sparkDfResult: Seq[Map[String, Any]],
                            differences: Map[String, (Any, Any)])

case class ValidationStats(totalRecords: Int, totalMatches: Int, totalMismatches: Int)

class ValidationFlinkJob(eventSrc: KafkaFlinkSource,
                         groupByServingInfoParsed: GroupByServingInfoParsed,
                         encoder: Encoder[Row],
                         parallelism: Int,
                         validationRows: Int) {
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

  def runValidationJob(env: StreamExecutionEnvironment): DataStream[ValidationStats] = {

    logger.info(s"Running Validation job for groupByName=$groupByName, Topic=$topic")

    // we expect parallelism on the source stream to be set by the source provider
    val sourceStream: DataStream[Row] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    // add a unique record ID to every record
    val sourceStreamWithId: DataStream[(Row, String)] = sourceStream
      .map((_, java.util.UUID.randomUUID().toString))
      .uid(s"source-with-id-$groupByName")
      .name(s"Source with ID for $groupByName")
      .setParallelism(sourceStream.getParallelism) // Use same parallelism as previous operator

    val exprEvalCU: SparkExpressionEvalFn[Row] =
      new SparkExpressionEvalFn[Row](encoder, groupByServingInfoParsed.groupBy)
    val exprEvalSparkDf: SparkExpressionEvalFn[Row] =
      new SparkExpressionEvalFn[Row](encoder, groupByServingInfoParsed.groupBy, useCatalyst = false)

    val comparisonStream: DataStream[ComparisonResult] = sourceStreamWithId
      .map { element =>
        val (event, id) = element
        val v1Results = new mutable.ArrayBuffer[Map[String, Any]]()
        val collector1 = new Collector[Map[String, Any]] {
          override def collect(result: Map[String, Any]): Unit = {
            v1Results += result
          }
          override def close(): Unit = {}
        }

        val v2Results = new mutable.ArrayBuffer[Map[String, Any]]()
        val collector2 = new Collector[Map[String, Any]] {
          override def collect(result: Map[String, Any]): Unit = {
            v2Results += result
          }
          override def close(): Unit = {}
        }

        exprEvalCU.flatMap(event, collector1)
        exprEvalSparkDf.flatMap(event, collector2)
        ComparisonResult(
          recordId = id,
          isMatch = v1Results.size == v2Results.size, // TODO update this!!
          catalystResult = v1Results,
          sparkDfResult = v2Results,
          differences = Map.empty
        )
      }
      .returns(TypeInformation.of(classOf[ComparisonResult]))
      .uid(s"cu-df-comparison-$groupByName")
      .name(s"Catalyst util spark df comparison for $groupByName")
      .setParallelism(sourceStream.getParallelism)

    comparisonStream
      .countWindowAll(validationRows)
      .apply { (window, input, out: Collector[ValidationStats]) =>
        var total = 0L
        var matching = 0L
        var mismatching = 0L
        input.asScala.foreach { result =>
          total += 1
          if (result.isMatch) {
            matching += 1
          } else {
            mismatching += 1
          }
        }
        out.collect(ValidationStats(total.toInt, matching.toInt, mismatching.toInt))
      }
      .returns(TypeInformation.of(classOf[ValidationStats]))
      .uid(s"validation-stats-$groupByName")
      .name(s"Validation stats for $groupByName")
      .setParallelism(1)
  }
}

object ValidateFlinkJob {
  def run(metadataStore: MetadataStore,
          kafkaBootstrap: Option[String],
          groupByName: String,
          validateRows: Int): Unit = {
    val maybeServingInfo = metadataStore.getGroupByServingInfo(groupByName)
    val validationJob: ValidationFlinkJob = maybeServingInfo
      .map { servingInfo =>
        val topicUri = servingInfo.groupBy.streamingSource.get.topic
        val topicInfo = TopicInfo.parse(topicUri)

        val schemaProvider =
          topicInfo.params.get(RegistryHostKey) match {
            case Some(_) => new SchemaRegistrySchemaProvider(topicInfo.params)
            case None =>
              throw new IllegalArgumentException(
                s"We only support schema registry based schema lookups. Missing $RegistryHostKey in topic config")
          }

        val (encoder, deserializationSchema) = schemaProvider.buildEncoderAndDeserSchema(topicInfo)
        val source =
          topicInfo.messageBus match {
            case "kafka" =>
              new KafkaFlinkSource(kafkaBootstrap, deserializationSchema, topicInfo)
            case _ =>
              throw new IllegalArgumentException(s"Unsupported message bus: ${topicInfo.messageBus}")
          }
        new ValidationFlinkJob(
          eventSrc = source,
          groupByServingInfoParsed = servingInfo,
          encoder = encoder,
          parallelism = source.parallelism,
          validationRows = validateRows
        )
      }
      .recover { case e: Exception =>
        throw new IllegalArgumentException(s"Unable to lookup serving info for GroupBy: '$groupByName'", e)
      }
      .get

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
      .enableForceKryo() // use kryo for complex types that Flink's default ser system doesn't support (e.g case classes)
    env.getConfig.enableGenericTypes() // more permissive type checks

    val jobDatastream = validationJob.runValidationJob(env)

    jobDatastream
      .print()
      .setParallelism(jobDatastream.getParallelism)

    env.execute(s"${validationJob.groupByName}")
  }
}
