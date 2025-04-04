package ai.chronon.orchestration.pubsub

import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName
import org.slf4j.LoggerFactory

import java.util.concurrent.{CompletableFuture, Executors}
import scala.util.{Failure, Success, Try}

/** Publisher interface for sending messages to a Pub/Sub system.
  *
  * This trait defines the core functionality for publishing messages to
  * a Pub/Sub topic, providing a clean abstraction over the underlying
  * Pub/Sub implementation.
  */
trait PubSubPublisher {

  /** Gets the topic ID this publisher publishes to.
    *
    * The topic ID uniquely identifies the destination for messages
    * published through this publisher.
    *
    * @return The unique identifier for the topic
    */
  def topicId: String

  /** Publishes a message to the Pub/Sub topic.
    *
    * This method asynchronously publishes a message to the topic and returns
    * a future that completes when the message is successfully published or
    * fails if there's an error.
    *
    * @param message The message to publish
    * @return A CompletableFuture that completes with the message ID when published successfully,
    *         or completes exceptionally if there's an error
    */
  def publish(message: PubSubMessage): CompletableFuture[String]

  /** Releases resources and shuts down the publisher.
    *
    * This method should be called when the publisher is no longer needed to
    * properly release resources and avoid leaks.
    */
  def shutdown(): Unit
}

/** Google Cloud implementation of the PubSubPublisher interface.
  *
  * @param config The Google Cloud Pub/Sub configuration
  * @param topicId The ID of the topic to publish to
  */
class GcpPubSubPublisher(
    val config: GcpPubSubConfig,
    val topicId: String
) extends PubSubPublisher {
  private val logger = LoggerFactory.getLogger(getClass)
  private val executor = Executors.newSingleThreadExecutor()
  private lazy val publisher = createPublisher()

  protected def createPublisher(): Publisher = {
    val topicName = TopicName.of(config.projectId, topicId)
    logger.info(s"Creating publisher for topic: $topicName")

    // Start with the basic builder
    val builder = Publisher.newBuilder(topicName)

    // Add channel provider if specified
    config.channelProvider.foreach { provider =>
      logger.info("Using custom channel provider for Publisher")
      builder.setChannelProvider(provider)
    }

    // Add credentials provider if specified
    config.credentialsProvider.foreach { provider =>
      logger.info("Using custom credentials provider for Publisher")
      builder.setCredentialsProvider(provider)
    }

    // Build the publisher
    builder.build()
  }

  override def publish(message: PubSubMessage): CompletableFuture[String] = {
    val result = new CompletableFuture[String]()

    message match {
      case gcpMessage: GcpPubSubMessage =>
        Try {
          // Convert to Google PubSub message format
          val pubsubMessage = gcpMessage.toPubsubMessage

          // Publish the message
          val messageIdFuture = publisher.publish(pubsubMessage)

          // Add a callback to handle success/failure
          ApiFutures.addCallback(
            messageIdFuture,
            new ApiFutureCallback[String] {
              override def onFailure(t: Throwable): Unit = {
                logger.error(s"Failed to publish message to $topicId", t)
                result.completeExceptionally(t)
              }

              override def onSuccess(messageId: String): Unit = {
                logger.info(s"Published message with ID: $messageId to $topicId")
                result.complete(messageId)
              }
            },
            executor
          )
        } match {
          case Success(_) => // Callback will handle completion
          case Failure(e) =>
            logger.error(s"Error setting up message publishing to $topicId", e)
            result.completeExceptionally(e)
        }
      case _ =>
        val error = new IllegalArgumentException(
          s"Message type ${message.getClass.getName} is not supported for GCP PubSub. Expected GcpPubSubMessage.")
        logger.error(error.getMessage)
        result.completeExceptionally(error)
    }

    result
  }

  override def shutdown(): Unit = {
    Try {
      if (publisher != null) {
        publisher.shutdown()
      }

      executor.shutdown()

      logger.info(s"Publisher for topic $topicId shut down successfully")
    } match {
      case Success(_) => // Shutdown successful
      case Failure(e) => logger.error(s"Error shutting down publisher for topic $topicId", e)
    }
  }
}

/** Factory for creating PubSubPublisher instances.
  */
object PubSubPublisher {

  /** Creates a publisher for Google Cloud PubSub.
    *
    * This factory method creates a new PubSubPublisher instance configured
    * for the Google Cloud Pub/Sub service using the provided configuration.
    *
    * @param config The Google Cloud Pub/Sub configuration
    * @param topicId The ID of the topic to publish to
    * @return A configured PubSubPublisher instance
    */
  def apply(config: GcpPubSubConfig, topicId: String): PubSubPublisher = {
    new GcpPubSubPublisher(config, topicId)
  }

  /** Creates a publisher for the Pub/Sub emulator.
    *
    * This convenience method creates a publisher configured to work with
    * a local Pub/Sub emulator, which is useful for development and testing
    * without requiring actual Google Cloud resources.
    *
    * @param projectId The Google Cloud project ID (can be any value for emulator)
    * @param topicId The ID of the topic to publish to
    * @param emulatorHost The host and port of the Pub/Sub emulator (e.g., "localhost:8085")
    * @return A configured PubSubPublisher instance for the emulator
    */
  def forEmulator(projectId: String, topicId: String, emulatorHost: String): PubSubPublisher = {
    val config = GcpPubSubConfig.forEmulator(projectId, emulatorHost)
    apply(config, topicId)
  }
}
