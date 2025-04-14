package ai.chronon.orchestration.test.pubsub

import ai.chronon.orchestration.pubsub._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Try

/** Integration tests for PubSub components with the emulator.
  *
  * Prerequisites:
  * - PubSub emulator must be running
  * - PUBSUB_EMULATOR_HOST environment variable must be set (e.g., localhost:8085)
  */
class GcpPubSubIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Test configuration
  private val emulatorHost = sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")
  private val projectId = "test-project"
  private val testId = UUID.randomUUID().toString.take(8) // Generate unique IDs for tests
  private val topicId = s"integration-topic-$testId"
  private val subscriptionId = s"integration-sub-$testId"

  // Components under test
  private var pubSubManager: PubSubManager = _
  private var pubSubAdmin: PubSubAdmin = _
  private var publisher: PubSubPublisher = _
  private var subscriber: PubSubSubscriber = _

  override def beforeAll(): Unit = {
    // Check if the emulator is available
    assume(
      sys.env.contains("PUBSUB_EMULATOR_HOST"),
      "PubSub emulator not available. Set PUBSUB_EMULATOR_HOST environment variable."
    )

    // Create test configuration and components
    val config = GcpPubSubConfig.forEmulator(projectId, emulatorHost)
    pubSubManager = PubSubManager(config)
    pubSubAdmin = PubSubAdmin(config)

    // Create topic and subscription
    Try {
      pubSubAdmin.createTopic(topicId)
      pubSubAdmin.createSubscription(topicId, subscriptionId)
    }.recover { case e: Exception =>
      fail(s"Failed to set up PubSub resources: ${e.getMessage}")
    }

    // Get publisher and subscriber
    publisher = pubSubManager.getOrCreatePublisher(topicId)
    subscriber = pubSubManager.getOrCreateSubscriber(topicId, subscriptionId)
  }

  override def afterAll(): Unit = {
    // Clean up all resources
    Try {
      if (publisher != null) publisher.shutdown()
      if (subscriber != null) subscriber.shutdown()
      if (pubSubAdmin != null) pubSubAdmin.close()
      if (pubSubManager != null) pubSubManager.shutdown()
    }
  }

  "PubSubAdmin" should "create topics and subscriptions idempotent" in {
    // Create unique IDs for this test
    val testTopicId = s"topic-admin-test-${UUID.randomUUID().toString.take(8)}"
    val testSubId = s"sub-admin-test-${UUID.randomUUID().toString.take(8)}"

    // Create topic
    pubSubAdmin.createTopic(testTopicId)

    // Call again to test idempotence (should not throw error)
    pubSubAdmin.createTopic(testTopicId)

    // Create subscription
    pubSubAdmin.createSubscription(testTopicId, testSubId)

    // Call again to test idempotence (should not throw error)
    pubSubAdmin.createSubscription(testTopicId, testSubId)
  }

  "PubSubAdmin" should "handle creating multiple topics and subscriptions" in {
    // Create unique IDs for this test
    val testTopicId1 = s"topic-multi-1-${UUID.randomUUID().toString.take(8)}"
    val testTopicId2 = s"topic-multi-2-${UUID.randomUUID().toString.take(8)}"
    val testSubId1 = s"sub-multi-1-${UUID.randomUUID().toString.take(8)}"
    val testSubId2 = s"sub-multi-2-${UUID.randomUUID().toString.take(8)}"

    // Create multiple topics
    pubSubAdmin.createTopic(testTopicId1)
    pubSubAdmin.createTopic(testTopicId2)

    // Create multiple subscriptions
    pubSubAdmin.createSubscription(testTopicId1, testSubId1)
    pubSubAdmin.createSubscription(testTopicId2, testSubId2)
  }

  "PubSubPublisher and PubSubSubscriber" should "publish and receive messages" in {
    // Create a test message
    val message = JobSubmissionMessage(
      nodeName = "integration-test",
      data = Some("Test message for integration testing"),
      attributes = Map("test" -> "true")
    )

    // Publish the message
    val messageIdFuture = publisher.publish(message)
    val messageId = messageIdFuture.get(5, TimeUnit.SECONDS)
    messageId should not be null

    // Pull messages
    val messages = subscriber.pullMessages(10)
    messages.size should be(1)

    // Find our message
    val receivedMessage = findMessageByNodeName(messages, "integration-test")
    receivedMessage should be(defined)

    // Verify contents
    val pubsubMsg = receivedMessage.get
    pubsubMsg.getAttributes.getOrElse("nodeName", "") should be("integration-test")
    pubsubMsg.getAttributes.getOrElse("test", "") should be("true")
  }

  "PubSubManager" should "properly handle multiple publishers and subscribers" in {
    // Create unique IDs for this test
    val testTopicId = s"topic-multi-test-${UUID.randomUUID().toString.take(8)}"
    val testSubId1 = s"sub-multi-test-1-${UUID.randomUUID().toString.take(8)}"
    val testSubId2 = s"sub-multi-test-2-${UUID.randomUUID().toString.take(8)}"

    // Create topic and subscriptions
    pubSubAdmin.createTopic(testTopicId)
    pubSubAdmin.createSubscription(testTopicId, testSubId1)
    pubSubAdmin.createSubscription(testTopicId, testSubId2)

    // Get publishers and subscribers
    val testPublisher = pubSubManager.getOrCreatePublisher(testTopicId)
    val testSubscriber1 = pubSubManager.getOrCreateSubscriber(testTopicId, testSubId1)
    val testSubscriber2 = pubSubManager.getOrCreateSubscriber(testTopicId, testSubId2)

    // Publish a message
    val message = JobSubmissionMessage("multi-test", Some("Testing multiple subscribers"))
    testPublisher.publish(message).get(5, TimeUnit.SECONDS)

    // Both subscribers should receive the message
    val messages1 = testSubscriber1.pullMessages(10)
    val messages2 = testSubscriber2.pullMessages(10)

    // Verify messages from both subscribers
    findMessageByNodeName(messages1, "multi-test") should be(defined)
    findMessageByNodeName(messages2, "multi-test") should be(defined)
  }

  "PubSubPublisher" should "handle batch publishing" in {
    // Create and publish multiple messages
    val messageCount = 5
    val messageIds = (1 to messageCount).map { i =>
      val message = JobSubmissionMessage(s"batch-node-$i", Some(s"Batch message $i"))
      publisher.publish(message).get(5, TimeUnit.SECONDS)
    }

    // Verify all messages got IDs
    messageIds.size should be(messageCount)
    messageIds.foreach(_ should not be null)

    // Pull messages
    val messages = subscriber.pullMessages(messageCount + 5) // Add buffer

    // Verify all node names are present
    val foundNodeNames = messages.map(msg => msg.getAttributes.getOrElse("nodeName", "")).toSet

    // Check each batch message is found
    (1 to messageCount).foreach { i =>
      val nodeName = s"batch-node-$i"
      withClue(s"Missing message for node $nodeName: ") {
        foundNodeNames should contain(nodeName)
      }
    }
  }

  // Helper method to find a message by node name
  private def findMessageByNodeName(messages: Seq[PubSubMessage], nodeName: String): Option[PubSubMessage] = {
    messages.find(msg => msg.getAttributes.getOrElse("nodeName", "") == nodeName)
  }
}
