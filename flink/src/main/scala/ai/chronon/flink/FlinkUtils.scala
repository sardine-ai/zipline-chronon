package ai.chronon.flink

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{Accuracy, Builders, GroupBy, GroupByServingInfo, Operation, PartitionSpec, TimeUnit, Window}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.{FlinkSource, GroupByServingInfoParsed}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

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

object FlinkUtils {
  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    groupByServingInfo.setInputAvroSchema(
      inputSchema.toAvroSchema("Input").toString(true)
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
}