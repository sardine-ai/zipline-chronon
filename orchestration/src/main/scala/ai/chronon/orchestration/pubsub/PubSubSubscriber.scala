package ai.chronon.orchestration.pubsub

import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.orchestration.utils.GcpPubSubAdminUtils
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.pubsub.v1.{PubsubMessage, SubscriptionName}
import org.slf4j.LoggerFactory

/** Generic subscriber interface for receiving messages from a Pub/Sub system.
  *
  * This trait defines the core functionality for subscribing to and receiving
  * messages from a Pub/Sub system, providing a clean abstraction over the
  * underlying Pub/Sub implementation.
  */
trait PubSubSubscriber {
  private val batchSize = 10

  /** Gets the subscription ID this subscriber listens to.
    */
  def subscriptionId: String

  /** Pulls messages from the subscription.
    *
    * This method pulls a batch of messages from the subscription and automatically
    * acknowledges them upon successful retrieval. The default batch size is 10
    * messages, but this can be configured through the maxMessages parameter.
    *
    * @param maxMessages Maximum number of messages to pull in a single batch
    * @return A sequence of received messages
    * @throws RuntimeException if there's an error communicating with the subscription
    */
  def pullMessages(maxMessages: Int = batchSize): Seq[PubSubMessage]

  /** Releases resources and shuts down the subscriber.
    *
    * This method should be called when the subscriber is no longer needed to
    * properly release resources and avoid leaks.
    */
  def shutdown(): Unit
}

/** Google Cloud implementation of the PubSubSubscriber interface.
  *
  * @param config The Google Cloud Pub/Sub configuration
  * @param subscriptionId The ID of the subscription to pull messages from
  */
class GcpPubSubSubscriber(
    config: GcpPubSubConfig,
    val subscriptionId: String
) extends PubSubSubscriber {
  private val logger = LoggerFactory.getLogger(getClass)
  protected val adminClient: SubscriptionAdminClient = GcpPubSubAdminUtils.createSubscriptionAdminClient(config)

  override def pullMessages(maxMessages: Int): Seq[PubSubMessage] = {
    val subscriptionName = SubscriptionName.of(config.projectId, subscriptionId)

    try {
      val response = adminClient.pull(subscriptionName, maxMessages)

      val receivedMessages = response.getReceivedMessagesList.toScala

      // Convert to GCP-specific messages
      val messages = receivedMessages
        .map(received => {
          val pubsubMessage = received.getMessage

          // Convert to our abstraction with special wrapper for GCP messages
          new GcpPubSubMessageWrapper(pubsubMessage)
        })

      // Acknowledge the messages
      if (messages.nonEmpty) {
        try {
          val ackIds = receivedMessages
            .map(received => received.getAckId)

          adminClient.acknowledge(subscriptionName, ackIds.toJava)
        } catch {
          case e: Exception =>
            // Log the acknowledgment error but still return the messages
            logger.warn(s"Error acknowledging messages from $subscriptionId: ${e.getMessage}")
        }
      }

      messages
    } catch {
      // TODO: To add proper error handling based on other potential scenarios
      case e: Exception =>
        val errorMsg = s"Error pulling messages from $subscriptionId: ${e.getMessage}"
        logger.error(errorMsg)
        throw new RuntimeException(errorMsg, e)
    }
  }

  /** Releases resources and shuts down the subscriber.
    *
    * This method closes the Google Cloud Subscription Admin Client and
    * releases all associated resources to prevent leaks.
    */
  override def shutdown(): Unit = {
    // Close the admin client
    if (adminClient != null) {
      adminClient.close()
    }
    logger.info(s"Subscriber for subscription $subscriptionId shut down successfully")
  }
}

/** Wrapper for Google Cloud PubSub messages that implements our PubSubMessage abstraction.
  *
  * This class wraps a Google Cloud PubsubMessage to make it compatible with
  * our generic PubSubMessage interface.
  *
  * @param message The Google Cloud PubsubMessage to wrap
  */
class GcpPubSubMessageWrapper(val message: PubsubMessage) extends GcpPubSubMessage {

  override def getAttributes: Map[String, String] = {
    message.getAttributesMap.toScala.toMap
  }

  override def getData: Option[Array[Byte]] = {
    if (message.getData.isEmpty) None
    else Some(message.getData.toByteArray)
  }

  override def toPubsubMessage: PubsubMessage = message
}

/** Factory for creating PubSubSubscriber instances.
  */
object PubSubSubscriber {

  /** Creates a subscriber for Google Cloud PubSub.
    *
    * This factory method creates a new PubSubSubscriber instance configured
    * for the Google Cloud Pub/Sub service using the provided configuration.
    *
    * @param config The Google Cloud Pub/Sub configuration
    * @param subscriptionId The ID of the subscription to pull messages from
    * @return A configured PubSubSubscriber instance
    */
  def apply(
      config: GcpPubSubConfig,
      subscriptionId: String
  ): PubSubSubscriber = {
    new GcpPubSubSubscriber(config, subscriptionId)
  }
}
