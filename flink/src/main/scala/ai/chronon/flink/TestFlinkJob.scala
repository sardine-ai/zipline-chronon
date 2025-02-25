package ai.chronon.flink

import ai.chronon.api.Accuracy
import ai.chronon.api.Builders
import ai.chronon.api.DataType
import ai.chronon.api.DoubleType
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.GroupBy
import ai.chronon.api.GroupByServingInfo
import ai.chronon.api.IntType
import ai.chronon.api.LongType
import ai.chronon.api.Operation
import ai.chronon.api.PartitionSpec
import ai.chronon.api.StringType
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.api.{StructType => ApiStructType}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.Api
import ai.chronon.online.AvroConversions
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroCodec
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.SimpleUserCodeClassLoader
import org.apache.flink.util.UserCodeClassLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.AvroDeserializationSupport
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

// This file contains utility classes to spin up a TestFlink job to test end to end submission of Flink
// jobs to Flink clusters (e.g. DataProc). We mock things out at the moment to go with an in-memory
// datastream source as well as created a mocked GroupByServingInfo. The job does write out data to
// the configured KV store.

class E2EEventSource(mockEvents: Seq[Row], mockPartitionCount: Int) extends FlinkSource[Row] {

  implicit val parallelism: Int = mockPartitionCount

  override def getDataStream(topic: String, groupName: String)(env: StreamExecutionEnvironment,
                                                               parallelism: Int): SingleOutputStreamOperator[Row] = {
    env
      .addSource(new SourceFunction[Row] {
        private var isRunning = true

        override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
          while (isRunning) {
            mockEvents.foreach { event =>
              ctx.collect(event)
            }
            // Add some delay between event batches
            Thread.sleep(1000)
          }
        }

        override def cancel(): Unit = {
          isRunning = false
        }
      })
      .setParallelism(parallelism)
  }
}

class PrintSink extends SinkFunction[WriteResponse] {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    val elapsedTime = System.currentTimeMillis() - value.tsMillis
    logger.info(s"Received write response with status ${value.status}; elapsedTime = $elapsedTime ms")
  }
}

class DummyInitializationContext
    extends SerializationSchema.InitializationContext
    with DeserializationSchema.InitializationContext {
  override def getMetricGroup = new UnregisteredMetricsGroup

  override def getUserCodeClassLoader: UserCodeClassLoader =
    SimpleUserCodeClassLoader.create(classOf[DummyInitializationContext].getClassLoader)
}

object TestFlinkJob {
  val fields: Array[(String, DataType)] = Array(
    "id" -> StringType,
    "int_val" -> IntType,
    "double_val" -> DoubleType,
    "created" -> LongType
  )

  val e2eTestEventSchema: ApiStructType =
    ApiStructType.from("E2ETestEvent", fields)
  val e2eTestEventAvroSchema: String = AvroConversions.fromChrononSchema(e2eTestEventSchema).toString()
  val (avroRowEncoder, avroDeserializationSchema) =
    AvroDeserializationSupport.build("events.my_stream", e2eTestEventAvroSchema)

  def makeSource(mockPartitionCount: Int): FlinkSource[Row] = {
    val avroCodec = AvroCodec.of(e2eTestEventAvroSchema)
    avroDeserializationSchema.open(new DummyInitializationContext)
    val startTs = System.currentTimeMillis()
    val elements: Seq[Map[String, AnyRef]] = (0 until 10).map(i =>
      Map("id" -> s"test$i",
          "int_val" -> Integer.valueOf(i),
          "double_val" -> java.lang.Double.valueOf(i),
          "created" -> java.lang.Long.valueOf(startTs)))
    val rowElements = elements
      .map(valueMap => avroCodec.encode(valueMap))
      .map(avroDeserializationSchema.deserialize)
    new E2EEventSource(rowElements, mockPartitionCount)
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
        groupBy.keyColumns.toScala.map { keyCol =>
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
    val aggInputColNames = groupBy.aggregations.toScala.map(_.inputColumn).toList
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

  def buildTestFlinkJob(api: Api): FlinkJob[Row] = {
    val groupBy = makeGroupBy(Seq("id"))
    val parallelism = 1
    val e2EEventSource = makeSource(parallelism)

    val outputSchema = new SparkExpressionEvalFn(avroRowEncoder, groupBy).getOutputSchema
    val groupByServingInfoParsed = makeTestGroupByServingInfoParsed(groupBy, avroRowEncoder.schema, outputSchema)

    new FlinkJob(
      eventSrc = e2EEventSource,
      sinkFn = new AsyncKVStoreWriter(api, groupBy.metaData.name),
      groupByServingInfoParsed = groupByServingInfoParsed,
      encoder = avroRowEncoder,
      parallelism = parallelism
    )
  }
}
