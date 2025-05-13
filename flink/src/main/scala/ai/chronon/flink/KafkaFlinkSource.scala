package ai.chronon.flink

import ai.chronon.flink.deser.ChrononDeserializationSchema
import ai.chronon.online.TopicChecker
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.spark.sql.Row

class BaseKafkaFlinkSource[T](kafkaBootstrap: Option[String],
                              deserializationSchema: DeserializationSchema[T],
                              topicInfo: TopicInfo)
    extends FlinkSource[T] {
  val bootstrap: String =
    kafkaBootstrap.getOrElse(
      topicInfo.params.getOrElse(
        "bootstrap",
        topicInfo.params("host") + topicInfo.params
          .get("port")
          .map(":" + _)
          .getOrElse(throw new IllegalArgumentException("No bootstrap servers provided"))
      ))

  // confirm the topic exists
  TopicChecker.topicShouldExist(topicInfo.name, bootstrap, topicInfo.params)

  // we use a small scale factor as topics are often over partitioned. We can make this configurable via topicInfo
  val scaleFactor = 0.25

  implicit val parallelism: Int = {
    math.ceil(TopicChecker.getPartitions(topicInfo.name, bootstrap, topicInfo.params) * scaleFactor).toInt
  }

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {
    val kafkaSource = KafkaSource
      .builder[T]()
      .setTopics(topicInfo.name)
      .setGroupId(s"chronon-$groupByName")
      // we might have a fairly large backlog to catch up on, so we choose to go with the latest offset when we're
      // starting afresh
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(deserializationSchema)
      .setBootstrapServers(bootstrap)
      .setProperties(TopicChecker.mapToJavaProperties(topicInfo.params))
      .build()

    env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), s"Kafka source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
  }
}

class KafkaFlinkSource(kafkaBootstrap: Option[String],
                       deserializationSchema: ChrononDeserializationSchema[Row],
                       topicInfo: TopicInfo)
    extends BaseKafkaFlinkSource[Row](kafkaBootstrap, deserializationSchema, topicInfo)

class ProjectedKafkaFlinkSource(kafkaBootstrap: Option[String],
                                deserializationSchema: ChrononDeserializationSchema[Map[String, Any]],
                                topicInfo: TopicInfo)
    extends BaseKafkaFlinkSource[Map[String, Any]](kafkaBootstrap, deserializationSchema, topicInfo)
