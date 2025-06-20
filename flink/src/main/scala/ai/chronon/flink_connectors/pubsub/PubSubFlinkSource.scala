package ai.chronon.flink_connectors.pubsub

import ai.chronon.flink.source.{FlinkSource, FlinkSourceProvider}
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource

import java.time.Duration

/** Chronon Flink source that reads events from Google PubSub. Can be configured on the topic as:
  * pubsub://test-item-event-data/tasks=20/<other-params>
  *
  * Config params such as the project name and pubsub topic subscription are read from the online properties (configured in
  * teams.py or env variables).
  *
  * PubSub differs a bit from Kafka in a couple of aspects:
  * 1. Topic parallelism is managed internally - due to this we can't derive a default based on topic properties. We hardcode a
  * default and allow the user to override it via the 'tasks' property.
  * 2. PubSub subscriptions are created upfront and job restarts will always resume from the last ACKed message (done during checkpointing).
  * This means if the job is down for a while, we'll have a decent sized backlog to catch up on. To start afresh, a new subscription is
  * needed.
  */
class PubSubFlinkSource[T](props: Map[String, String],
                           deserializationSchema: DeserializationSchema[T],
                           topicInfo: TopicInfo)
    extends FlinkSource[T] {

  import PubSubFlinkSource._

  implicit val parallelism: Int =
    FlinkSourceProvider.getProperty(TaskParallelism, props, topicInfo).map(_.toInt).getOrElse(DefaultParallelism)

  // Defaults in the Pubsub source are 100 messages per pull, 15s timeout and 3 retries
  // we lower the timeout by default to 5s and allow users to override it
  private val messagesPerPull: Int = FlinkSourceProvider
    .getProperty(MessagesPerPull, props, topicInfo)
    .map(_.toInt)
    .getOrElse(100)

  private val maxRetries: Int = FlinkSourceProvider
    .getProperty(MaxRetries, props, topicInfo)
    .map(_.toInt)
    .getOrElse(3)

  private val pullTimeout: Duration =
    FlinkSourceProvider
      .getProperty(PullTimeoutMillis, props, topicInfo)
      .map(t => Duration.ofMillis(t.toLong))
      .getOrElse(Duration.ofMillis(5000))

  val projectName: String = getOrThrow(GcpProject, props)
  val subscriptionName: String = FlinkSourceProvider
    .getProperty(SubscriptionName, props, topicInfo)
    .getOrElse(throw new IllegalArgumentException(s"Missing required property: $SubscriptionName"))

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {
    val pubSubSrc: PubSubSource[T] = PubSubSource
      .newBuilder()
      .withDeserializationSchema(deserializationSchema)
      .withProjectName(projectName)
      .withSubscriptionName(subscriptionName)
      .withPubSubSubscriberFactory(messagesPerPull, pullTimeout, maxRetries)
      .build()

    // skip watermarks at the source as we derive them post Spark expr eval
    val noWatermarks: WatermarkStrategy[T] = WatermarkStrategy.noWatermarks()
    env
      .addSource(pubSubSrc, s"PubSub source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
      .uid(s"pubsub-source-$groupByName")
      .assignTimestampsAndWatermarks(noWatermarks)
  }
}

object PubSubFlinkSource {
  val GcpProject = "GCP_PROJECT_ID"
  val SubscriptionName = "subscription"
  val TaskParallelism = "tasks"
  val MessagesPerPull = "messages_per_pull"
  val MaxRetries = "max_retries"
  val PullTimeoutMillis = "pull_timeout_ms"

  // go with a default //ism of 20 as that gives us some room to handle decent load without
  // too many tasks
  val DefaultParallelism = 20

  private def getOrThrow(key: String, props: Map[String, String]): String = {
    props.getOrElse(key, throw new IllegalArgumentException(s"Missing required property: $key"))
  }
}
