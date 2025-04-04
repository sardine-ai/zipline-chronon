package ai.chronon.orchestration.pubsub

import ai.chronon.orchestration.utils.GcpPubSubAdminUtils
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, TopicAdminClient}
import com.google.pubsub.v1.{PushConfig, SubscriptionName, TopicName}
import org.slf4j.LoggerFactory

/** Administrative interface for managing Pub/Sub topics and subscriptions.
  *
  * This trait defines operations for creating and managing Pub/Sub resources,
  * providing a clean abstraction over the underlying Pub/Sub implementation.
  * It handles:
  *
  * - Topic creation and management
  * - Subscription creation and management
  * - Resource cleanup
  *
  * The interface is designed to be implementation-agnostic, allowing for
  * different Pub/Sub backends to be used interchangeably.
  */
trait PubSubAdmin {

  /** Creates a topic in the Pub/Sub system if it doesn't already exist.
    *
    * This method attempts to create the topic and:
    * - Succeeds if the topic is created successfully
    * - Ignores the error if the topic already exists (making it idempotent)
    * - Throws an exception for any other error
    *
    * @param topicId The unique identifier for the topic
    * @throws Exception If there's an error creating the topic (other than 'already exists')
    */
  def createTopic(topicId: String): Unit

  /** Creates a subscription to a topic if it doesn't already exist.
    *
    * This method attempts to create the subscription and:
    * - Succeeds if the subscription is created successfully
    * - Ignores the error if the subscription already exists (making it idempotent)
    * - Throws an exception for any other error
    *
    * @param topicId The topic ID to subscribe to
    * @param subscriptionId The unique identifier for the subscription
    * @throws Exception If there's an error creating the subscription (other than 'already exists')
    */
  def createSubscription(topicId: String, subscriptionId: String): Unit

  /** Releases resources and closes all admin clients.
    *
    * This method should be called when the admin is no longer needed to
    * properly release resources and avoid leaks.
    */
  def close(): Unit
}

/** Google Cloud Pub/Sub implementation of the PubSubAdmin interface.
  *
  * This class provides Google Cloud-specific implementation of Pub/Sub administrative
  * operations, using the Google Cloud Pub/Sub Admin API clients.
  *
  * This implementation uses a configuration that can be configured for either
  * production use with real GCP credentials or local emulator use.
  *
  * @param config The Google Cloud Pub/Sub configuration to use
  */
class GcpPubSubAdmin(config: GcpPubSubConfig) extends PubSubAdmin {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ackDeadlineSeconds = 10
  protected lazy val topicAdminClient: TopicAdminClient = GcpPubSubAdminUtils.createTopicAdminClient(config)
  protected lazy val subscriptionAdminClient: SubscriptionAdminClient =
    GcpPubSubAdminUtils.createSubscriptionAdminClient(config)

  override def createTopic(topicId: String): Unit = {
    val topicName = TopicName.of(config.projectId, topicId)

    try {
      topicAdminClient.createTopic(topicName)
      logger.info(s"Created topic: ${topicName.toString}")
    } catch {
      case e: Exception =>
        // If the topic already exists, log it as info rather than error
        if (e.getMessage != null && e.getMessage.contains("ALREADY_EXISTS")) {
          logger.info(s"Topic $topicId already exists, skipping creation")
        } else {
          logger.error(s"Error creating topic ${topicName.toString}: ${e.getMessage}")
          throw e
        }
    }
  }

  override def createSubscription(topicId: String, subscriptionId: String): Unit = {
    val topicName = TopicName.of(config.projectId, topicId)
    val subscriptionName = SubscriptionName.of(config.projectId, subscriptionId)

    try {
      subscriptionAdminClient.createSubscription(
        subscriptionName,
        topicName,
        PushConfig.getDefaultInstance, // Pull subscription
        ackDeadlineSeconds
      )
      logger.info(s"Created subscription: ${subscriptionName.toString}")
    } catch {
      case e: Exception =>
        // If the subscription already exists, log it as info rather than error
        if (e.getMessage != null && e.getMessage.contains("ALREADY_EXISTS")) {
          logger.info(s"Subscription $subscriptionId already exists for $topicId, skipping creation")
        } else {
          logger.error(s"Error creating subscription ${subscriptionName.toString}: ${e.getMessage}")
          throw e
        }
    }
  }

  override def close(): Unit = {
    try {
      if (topicAdminClient != null) {
        topicAdminClient.shutdown()
      }

      if (subscriptionAdminClient != null) {
        subscriptionAdminClient.shutdown()
      }

      logger.info("PubSub admin clients shut down successfully")
    } catch {
      case e: Exception =>
        logger.error("Error shutting down PubSub admin clients", e)
    }
  }
}

/** Factory for creating PubSubAdmin instances.
  *
  * This object provides factory methods to create different types of PubSubAdmin
  * implementations with appropriate configurations. It simplifies the creation of
  * admin instances for both production and local development environments.
  *
  * Usage examples:
  *
  * For production
  * val admin = PubSubAdmin(GcpPubSubConfig.forProduction("my-project"))
  *
  * For local emulator
  * val emulatorAdmin = PubSubAdmin.forEmulator("test-project", "localhost:8085")
  */
object PubSubAdmin {

  /** Creates a new Google Cloud PubSubAdmin instance with the provided configuration.
    *
    * @param config The Google Cloud Pub/Sub configuration
    * @return A new PubSubAdmin instance
    */
  def apply(config: GcpPubSubConfig): PubSubAdmin = {
    new GcpPubSubAdmin(config)
  }

  /** Creates a PubSubAdmin configured for use with the Pub/Sub emulator.
    *
    * This is a convenience method for local development and testing,
    * as it automatically configures the necessary settings for emulator use.
    *
    * @param projectId The project ID to use with the emulator
    * @param emulatorHost The emulator host:port (e.g., "localhost:8085")
    * @return A PubSubAdmin configured for emulator use
    */
  def forEmulator(projectId: String, emulatorHost: String): PubSubAdmin = {
    val config = GcpPubSubConfig.forEmulator(projectId, emulatorHost)
    apply(config)
  }
}
