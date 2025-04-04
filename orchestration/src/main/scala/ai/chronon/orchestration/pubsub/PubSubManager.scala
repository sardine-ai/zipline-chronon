package ai.chronon.orchestration.pubsub

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/** Central manager for Pub/Sub publishers and subscribers.
  *
  * This trait defines a manager that coordinates Pub/Sub components, providing
  * a single point of access for creating and retrieving publishers and subscribers.
  * It serves as a factory and registry for Pub/Sub components, ensuring:
  *
  * - Consistent configuration across components
  * - Resource reuse (clients are cached and shared)
  * - Proper resource lifecycle management
  * - Automatic topic and subscription creation
  *
  * Using this manager simplifies Pub/Sub operations by handling the infrastructure
  * details, allowing application code to focus on business logic.
  */
trait PubSubManager {

  /** Gets or creates a publisher for a specific topic.
    *
    * This method ensures that a topic exists before returning a publisher,
    * creating it if necessary. It also caches publishers to avoid creating
    * multiple instances for the same topic.
    *
    * @param topicId The unique identifier for the topic
    * @return A publisher configured for the specified topic
    */
  def getOrCreatePublisher(topicId: String): PubSubPublisher

  /** Gets or creates a subscriber for a specific subscription.
    *
    * This method ensures that both the topic and subscription exist before
    * returning a subscriber, creating them if necessary. It also caches
    * subscribers to avoid creating multiple instances for the same subscription.
    *
    * @param topicId The topic ID to subscribe to
    * @param subscriptionId The unique identifier for the subscription
    * @return A subscriber configured for the specified subscription
    */
  def getOrCreateSubscriber(topicId: String, subscriptionId: String): PubSubSubscriber

  /** Releases all resources managed by this manager.
    *
    * This method should be called when the manager is no longer needed to
    * properly close all publishers, subscribers, and other resources.
    * It ensures clean shutdown of connections and prevents resource leaks.
    */
  def shutdown(): Unit
}

/** Google Cloud implementation of the PubSubManager interface.
  *
  * The implementation uses thread-safe caching to ensure efficient resource use
  * while maintaining thread safety for concurrent operations.
  *
  * @param config The Google Cloud Pub/Sub configuration to use
  */
class GcpPubSubManager(config: GcpPubSubConfig) extends PubSubManager {
  private val logger = LoggerFactory.getLogger(getClass)
  protected val admin: PubSubAdmin = PubSubAdmin(config)

  // Cache of publishers by topic ID
  private val publishers = TrieMap.empty[String, PubSubPublisher]

  // Cache of subscribers by subscription ID
  private val subscribers = TrieMap.empty[String, PubSubSubscriber]

  override def getOrCreatePublisher(topicId: String): PubSubPublisher = {
    publishers.getOrElseUpdate(topicId, {
                                 // Create the topic if it doesn't exist
                                 admin.createTopic(topicId)

                                 // Create a new publisher
                                 PubSubPublisher(config, topicId)
                               })
  }

  override def getOrCreateSubscriber(topicId: String, subscriptionId: String): PubSubSubscriber = {
    subscribers.getOrElseUpdate(
      subscriptionId, {
        // Create the subscription if it doesn't exist
        admin.createSubscription(topicId, subscriptionId)

        // Create a new subscriber
        PubSubSubscriber(config, subscriptionId)
      }
    )
  }

  override def shutdown(): Unit = {
    try {
      // Shutdown all publishers
      publishers.values.foreach { publisher =>
        try {
          publisher.shutdown()
        } catch {
          case e: Exception =>
            logger.error(s"Error shutting down publisher: ${e.getMessage}")
        }
      }

      // Shutdown all subscribers
      subscribers.values.foreach { subscriber =>
        try {
          subscriber.shutdown()
        } catch {
          case e: Exception =>
            logger.error(s"Error shutting down subscriber: ${e.getMessage}")
        }
      }

      // Close the admin client
      admin.close()

      // Clear the caches
      publishers.clear()
      subscribers.clear()

      logger.info("PubSub manager shut down successfully")
    } catch {
      case e: Exception =>
        logger.error("Error shutting down PubSub manager", e)
    }
  }
}

/** Factory for creating and managing PubSubManager instances.
  *
  * This object provides factory methods for creating and retrieving PubSubManager
  * instances. It uses a cache to ensure that only one manager instance is created
  * for each unique configuration, promoting efficient resource usage.
  */
object PubSubManager {
  // Thread-safe cache of managers by configuration ID
  private val managers = TrieMap.empty[String, PubSubManager]

  /** Gets or creates a Google Cloud PubSubManager for a specific configuration.
    *
    * @param config The Google Cloud Pub/Sub configuration
    * @return A PubSubManager instance for the given configuration
    */
  def apply(config: GcpPubSubConfig): PubSubManager = {
    managers.getOrElseUpdate(config.id, new GcpPubSubManager(config))
  }

  /** Creates a manager configured for production Google Cloud environments.
    *
    * This convenience method creates a manager with production settings,
    * using default Google Cloud credentials in the specified project.
    *
    * @param projectId The Google Cloud project ID
    * @return A manager configured for production use
    */
  def forProduction(projectId: String): PubSubManager = {
    val config = GcpPubSubConfig.forProduction(projectId)
    apply(config)
  }

  /** Creates a manager configured for the local Pub/Sub emulator.
    *
    * This convenience method creates a manager for development and testing
    * with a local Pub/Sub emulator, automatically configuring the connection
    * properties.
    *
    * @param projectId The project ID to use with the emulator
    * @param emulatorHost The emulator host:port address
    * @return A manager configured for emulator use
    */
  def forEmulator(projectId: String, emulatorHost: String): PubSubManager = {
    val config = GcpPubSubConfig.forEmulator(projectId, emulatorHost)
    apply(config)
  }

  /** Shuts down all manager instances and clears the cache.
    *
    * This method provides a convenient way to clean up all Pub/Sub resources
    * when the application is shutting down. It should be called before the
    * application exits to ensure proper resource cleanup.
    */
  def shutdownAll(): Unit = {
    managers.values.foreach(_.shutdown())
    managers.clear()
  }
}
