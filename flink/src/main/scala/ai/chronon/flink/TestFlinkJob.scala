package ai.chronon.flink

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{Accuracy, Builders, GroupBy, GroupByServingInfo, Operation, PartitionSpec, TimeUnit, Window}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.{Api, FlinkSource, GroupByServingInfoParsed}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters.asScalaBufferConverter

//
// This file contains utility classes to spin up a TestFlink job to test end to end submission of Flink
// jobs to Flink clusters (e.g. DataProc). We mock things out at the moment to go with an in-memory
// datastream source as well as created a mocked GroupByServingInfo. The job does write out data to
// the configured KV store.

case class E2ETestEvent(id: String, int_val: Int, double_val: Double, created: Long)

class E2EEventSource(mockEvents: Seq[E2ETestEvent]) extends FlinkSource[E2ETestEvent] {

  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): DataStream[E2ETestEvent] = {
    env.fromCollection(mockEvents)
  }
}

class PrintSink extends SinkFunction[WriteResponse] {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    val elapsedTime = System.currentTimeMillis() - value.putRequest.tsMillis.get
    logger.info(s"Received write response with status ${value.status}; elapsedTime = $elapsedTime ms")
  }
}

class MockedEncoderProvider extends EncoderProvider[E2ETestEvent] {
  override def buildEncoder(): Encoder[E2ETestEvent] = Encoders.product[E2ETestEvent]
}

class MockedSourceProvider extends SourceProvider[E2ETestEvent](None) {
  import TestFlinkJob._

  override def buildSource(): (FlinkSource[E2ETestEvent], Int) = {
    val eventSrc = makeSource()
    val parallelism = 2 // TODO - take parallelism as a job param

    (eventSrc, parallelism)
  }
}

object TestFlinkJob {
  def makeSource(): FlinkSource[E2ETestEvent] = {
    val startTs = System.currentTimeMillis()
    val elements = (0 until 10).map(i => E2ETestEvent(s"test$i", i, i.toDouble, startTs))
    new E2EEventSource(elements)
  }

  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: StructType,
                                       outputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    groupByServingInfo.setInputAvroSchema(
      inputSchema.toAvroSchema("Input").toString(true)
    )

    // Set key avro schema for groupByServingInfo
    groupByServingInfo.setKeyAvroSchema(
      StructType(
        groupBy.keyColumns.asScala.map { keyCol =>
          val keyColStructType = outputSchema.fields.find(field => field.name == keyCol)
          keyColStructType match {
            case Some(col) => col
            case None =>
              throw new IllegalArgumentException(s"Missing key col from output schema: $keyCol")
          }
        }
      ).toAvroSchema("Key")
        .toString(true)
    )

    // Set value avro schema for groupByServingInfo
    val aggInputColNames = groupBy.aggregations.asScala.map(_.inputColumn).toList
    groupByServingInfo.setSelectedAvroSchema(
      StructType(outputSchema.fields.filter(field => aggInputColNames.contains(field.name)))
        .toAvroSchema("Value")
        .toString(true)
    )

    new GroupByServingInfoParsed(
      groupByServingInfo,
      PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
    )
  }

  def makeGroupBy(keyColumns: Seq[String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = filters,
            timeColumn = "created",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = keyColumns,
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        name = "e2e-count"
      ),
      accuracy = Accuracy.TEMPORAL
    )

  def buildTestFlinkJob(api: Api): FlinkJob[E2ETestEvent] = {
    val encoderProvider = new MockedEncoderProvider()
    val encoder = encoderProvider.buildEncoder()

    val groupBy = makeGroupBy(Seq("id"))
    val sourceProvider = new MockedSourceProvider()
    val (eventSrc, parallelism) = sourceProvider.buildSource()

    val outputSchema = new SparkExpressionEvalFn(encoder, groupBy).getOutputSchema
    val groupByServingInfoParsed = makeTestGroupByServingInfoParsed(groupBy, encoder.schema, outputSchema)

    new FlinkJob(
      eventSrc = eventSrc,
      sinkFn = new AsyncKVStoreWriter(api, groupBy.metaData.name),
      groupByServingInfoParsed = groupByServingInfoParsed,
      encoder = encoder,
      parallelism = parallelism
    )
  }
}