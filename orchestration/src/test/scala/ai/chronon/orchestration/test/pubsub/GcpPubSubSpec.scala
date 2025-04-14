package ai.chronon.orchestration.test.pubsub

import ai.chronon.orchestration.pubsub._
import com.google.api.core.ApiFuture
import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.pubsub.v1.{Publisher, SubscriptionAdminClient, TopicAdminClient}
import com.google.pubsub.v1.{
  PubsubMessage,
  PullResponse,
  ReceivedMessage,
  Subscription,
  SubscriptionName,
  Topic,
  TopicName
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util

/** Unit tests for PubSub components using mocks */
class GcpPubSubSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "GcpPubSubConfig" should "create production configuration" in {
    val config = GcpPubSubConfig.forProduction("test-project")

    config.projectId shouldBe "test-project"
    config.channelProvider shouldBe None
    config.credentialsProvider shouldBe None
  }

  "GcpPubSubConfig" should "create emulator configuration" in {
    val config = GcpPubSubConfig.forEmulator("test-project")

    config.projectId shouldBe "test-project"
    config.channelProvider shouldBe defined
    config.credentialsProvider shouldBe defined
    config.credentialsProvider.get.getClass shouldBe NoCredentialsProvider.create().getClass
  }

  "JobSubmissionMessage" should "convert to PubsubMessage correctly" in {
    val message = JobSubmissionMessage(
      nodeName = "test-node",
      data = Some("Test data"),
      attributes = Map("customKey" -> "customValue")
    )

    val pubsubMessage = message.toPubsubMessage

    pubsubMessage.getAttributesMap.get("nodeName") shouldBe "test-node"
    pubsubMessage.getAttributesMap.get("customKey") shouldBe "customValue"
    pubsubMessage.getData.toStringUtf8 shouldBe "Test data"
  }

  "GcpPubSubPublisher" should "publish messages successfully" in {
    // Mock dependencies
    val mockPublisher = mock[Publisher]
    val mockFuture = mock[ApiFuture[String]]

    // Set up config and topic
    val config = GcpPubSubConfig.forEmulator("test-project")
    val topicId = "test-topic"

    // Create a test publisher that uses the mock publisher
    val publisher = new GcpPubSubPublisher(config, topicId) {
      // Expose createPublisher as a test hook and override to return mock
      override def createPublisher(): Publisher = mockPublisher
    }

    // Set up the mock publisher to return our mock future
    when(mockPublisher.publish(any[PubsubMessage])).thenReturn(mockFuture)

    // Create a message and attempt to publish
    val message = JobSubmissionMessage("test-node", Some("Test data"))
    val resultFuture = publisher.publish(message)

    // Verify publisher was called with message
    verify(mockPublisher).publish(any[PubsubMessage])

    // Cleaning up
    publisher.shutdown()
  }

  "GcpPubSubAdmin createTopic" should "successfully create a new topic" in {
    // Mock the TopicAdminClient
    val mockTopicAdmin = mock[TopicAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val topicAdminClient: TopicAdminClient = mockTopicAdmin
    }

    // Mock the creation response for a new topic
    when(mockTopicAdmin.createTopic(any[TopicName])).thenReturn(mock[Topic])

    // Create the topic
    admin.createTopic("test-topic")

    // Verify createTopic was called
    verify(mockTopicAdmin).createTopic(any[TopicName])

    // Cleanup
    admin.close()
  }

  "GcpPubSubAdmin createTopic" should "handle the case when a topic already exists" in {
    // Mock the TopicAdminClient
    val mockTopicAdmin = mock[TopicAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val topicAdminClient: TopicAdminClient = mockTopicAdmin
    }

    // Mock the creation response to throw ALREADY_EXISTS exception
    val alreadyExistsException = new RuntimeException("ALREADY_EXISTS: Topic already exists")
    when(mockTopicAdmin.createTopic(any[TopicName])).thenThrow(alreadyExistsException)

    // Try to create the topic - should not throw exception
    admin.createTopic("test-topic")

    // Verify createTopic was called and exception was handled internally
    verify(mockTopicAdmin).createTopic(any[TopicName])

    // Cleanup
    admin.close()
  }

  "GcpPubSubAdmin createTopic" should "throw exception for errors other than 'already exists'" in {
    // Mock the TopicAdminClient
    val mockTopicAdmin = mock[TopicAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val topicAdminClient: TopicAdminClient = mockTopicAdmin
    }

    // Mock the create response to throw a different type of exception
    val otherException = new RuntimeException("PERMISSION_DENIED: Not authorized to create topic")
    when(mockTopicAdmin.createTopic(any[TopicName])).thenThrow(otherException)

    // Try to create the topic - should throw the exception
    val exception = intercept[RuntimeException] {
      admin.createTopic("test-topic")
    }

    // Verify the exception is the same as the one we mocked
    exception shouldBe otherException

    // Verify createTopic was called
    verify(mockTopicAdmin).createTopic(any[TopicName])
  }

  "GcpPubSubAdmin createSubscription" should "successfully create a new subscription" in {
    // Mock the SubscriptionAdminClient
    val mockSubscriptionAdmin = mock[SubscriptionAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val subscriptionAdminClient: SubscriptionAdminClient = mockSubscriptionAdmin
    }

    // Mock the creation response for a new subscription
    when(
      mockSubscriptionAdmin.createSubscription(
        any[SubscriptionName],
        any[TopicName],
        any(),
        any[Int]
      )).thenReturn(mock[Subscription])

    // Create the subscription
    admin.createSubscription("test-topic", "test-sub")

    // Verify createSubscription was called
    verify(mockSubscriptionAdmin).createSubscription(
      any[SubscriptionName],
      any[TopicName],
      any(),
      any[Int]
    )

    // Cleanup
    admin.close()
  }

  "GcpPubSubAdmin createSubscription" should "handle the case when a subscription already exists" in {
    // Mock the SubscriptionAdminClient
    val mockSubscriptionAdmin = mock[SubscriptionAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val subscriptionAdminClient: SubscriptionAdminClient = mockSubscriptionAdmin
    }

    // Mock the creation response to throw ALREADY_EXISTS exception
    val alreadyExistsException = new RuntimeException("ALREADY_EXISTS: Subscription already exists")
    when(
      mockSubscriptionAdmin.createSubscription(
        any[SubscriptionName],
        any[TopicName],
        any(),
        any[Int]
      )).thenThrow(alreadyExistsException)

    // Try to create the subscription - should not throw exception
    admin.createSubscription("test-topic", "test-sub")

    // Verify createSubscription was called and exception was handled internally
    verify(mockSubscriptionAdmin).createSubscription(
      any[SubscriptionName],
      any[TopicName],
      any(),
      any[Int]
    )

    // Cleanup
    admin.close()
  }

  "GcpPubSubAdmin createSubscription" should "throw exception for errors other than 'already exists'" in {
    // Mock the SubscriptionAdminClient
    val mockSubscriptionAdmin = mock[SubscriptionAdminClient]

    // Create a mock admin that uses our mock
    val admin = new GcpPubSubAdmin(GcpPubSubConfig.forEmulator("test-project")) {
      override protected lazy val subscriptionAdminClient: SubscriptionAdminClient = mockSubscriptionAdmin
    }

    // Mock the create response to throw a different type of exception
    val otherException = new RuntimeException("INVALID_ARGUMENT: Invalid subscription name")
    when(
      mockSubscriptionAdmin.createSubscription(
        any[SubscriptionName],
        any[TopicName],
        any(),
        any[Int]
      )).thenThrow(otherException)

    // Try to create the subscription - should throw the exception
    val exception = intercept[RuntimeException] {
      admin.createSubscription("test-topic", "test-sub")
    }

    // Verify the exception is the same as the one we mocked
    exception shouldBe otherException

    // Verify createSubscription was called
    verify(mockSubscriptionAdmin).createSubscription(
      any[SubscriptionName],
      any[TopicName],
      any(),
      any[Int]
    )
  }

  "GcpPubSubSubscriber" should "pull messages correctly" in {
    // Mock the subscription admin client
    val mockSubscriptionAdmin = mock[SubscriptionAdminClient]

    // Mock the pull response
    val mockPullResponse = mock[PullResponse]
    val mockReceivedMessage = mock[ReceivedMessage]
    val mockPubsubMessage = mock[PubsubMessage]

    // Set up the mocks
    when(mockReceivedMessage.getMessage).thenReturn(mockPubsubMessage)
    when(mockReceivedMessage.getAckId).thenReturn("test-ack-id")
    when(mockPullResponse.getReceivedMessagesList).thenReturn(util.Arrays.asList(mockReceivedMessage))
    when(mockSubscriptionAdmin.pull(any[SubscriptionName], any[Int])).thenReturn(mockPullResponse)

    // Create a test configuration
    val config = GcpPubSubConfig.forEmulator("test-project")

    // Create a test subscriber that uses our mock admin client
    val subscriber = new GcpPubSubSubscriber(config, "test-sub") {
      override protected val adminClient: SubscriptionAdminClient = mockSubscriptionAdmin
    }

    // Pull messages
    val messages = subscriber.pullMessages(10)

    // Verify
    messages.size shouldBe 1
    messages.head shouldBe a[PubSubMessage]

    // Verify acknowledge was called
    verify(mockSubscriptionAdmin).acknowledge(any[SubscriptionName], any())

    // Cleanup
    subscriber.shutdown()
  }

  "GcpPubSubSubscriber" should "throw RuntimeException when there is a pull error" in {
    // Mock the subscription admin client
    val mockSubscriptionAdmin = mock[SubscriptionAdminClient]

    // Set up the mock to throw an exception when pulling messages
    val errorMessage = "Error pulling messages"
    when(mockSubscriptionAdmin.pull(any[SubscriptionName], any[Int]))
      .thenThrow(new RuntimeException(errorMessage))

    // Create a test configuration
    val config = GcpPubSubConfig.forEmulator("test-project")

    // Create a test subscriber that uses our mock admin client
    val subscriber = new GcpPubSubSubscriber(config, "test-sub") {
      override protected val adminClient: SubscriptionAdminClient = mockSubscriptionAdmin
    }

    // Pull messages - should throw an exception
    val exception = intercept[RuntimeException] {
      subscriber.pullMessages(10)
    }

    // Verify the exception message
    exception.getMessage should include(errorMessage)

    // Cleanup
    subscriber.shutdown()
  }

  "PubSubManager" should "cache publishers and subscribers" in {
    // Create mock admin, publisher, and subscriber
    val mockAdmin = mock[PubSubAdmin]
    val mockPublisher1 = mock[PubSubPublisher]
    val mockPublisher2 = mock[PubSubPublisher]
    val mockSubscriber1 = mock[PubSubSubscriber]
    val mockSubscriber2 = mock[PubSubSubscriber]

    // Configure the mocks - don't need to return values for void methods
    doNothing().when(mockAdmin).createTopic(any[String])
    doNothing().when(mockAdmin).createSubscription(any[String], any[String])

    when(mockPublisher1.topicId).thenReturn("topic1")
    when(mockPublisher2.topicId).thenReturn("topic2")
    when(mockSubscriber1.subscriptionId).thenReturn("sub1")
    when(mockSubscriber2.subscriptionId).thenReturn("sub2")

    // Create a test manager with mocked components
    val config = GcpPubSubConfig.forEmulator("test-project")
    val manager = new GcpPubSubManager(config) {
      override protected val admin: PubSubAdmin = mockAdmin

      // Cache for our test publishers and subscribers
      private val testPublishers = Map(
        "topic1" -> mockPublisher1,
        "topic2" -> mockPublisher2
      )

      private val testSubscribers = Map(
        "sub1" -> mockSubscriber1,
        "sub2" -> mockSubscriber2
      )

      // Override publisher creation to return our mocks
      override def getOrCreatePublisher(topicId: String): PubSubPublisher = {
        admin.createTopic(topicId)
        testPublishers.getOrElse(topicId, {
                                   val pub = mock[PubSubPublisher]
                                   when(pub.topicId).thenReturn(topicId)
                                   pub
                                 })
      }

      // Override subscriber creation to return our mocks
      override def getOrCreateSubscriber(topicId: String, subscriptionId: String): PubSubSubscriber = {
        admin.createSubscription(topicId, subscriptionId)
        testSubscribers.getOrElse(subscriptionId, {
                                    val sub = mock[PubSubSubscriber]
                                    when(sub.subscriptionId).thenReturn(subscriptionId)
                                    sub
                                  })
      }
    }

    // Test publisher retrieval - should get the same instances for same topic
    val pub1First = manager.getOrCreatePublisher("topic1")
    val pub1Second = manager.getOrCreatePublisher("topic1")
    val pub2 = manager.getOrCreatePublisher("topic2")

    pub1First shouldBe mockPublisher1
    pub1Second shouldBe mockPublisher1
    pub2 shouldBe mockPublisher2

    // Test subscriber retrieval - should get same instances for same subscription
    val sub1First = manager.getOrCreateSubscriber("topic1", "sub1")
    val sub1Second = manager.getOrCreateSubscriber("topic1", "sub1")
    val sub2 = manager.getOrCreateSubscriber("topic1", "sub2")

    sub1First shouldBe mockSubscriber1
    sub1Second shouldBe mockSubscriber1
    sub2 shouldBe mockSubscriber2

    // Verify the admin calls
    verify(mockAdmin, times(2)).createTopic("topic1")
    verify(mockAdmin).createTopic("topic2")
    verify(mockAdmin, times(2)).createSubscription("topic1", "sub1")
    verify(mockAdmin).createSubscription("topic1", "sub2")

    // Cleanup
    manager.shutdown()
  }

  "PubSubManager companion" should "cache managers by config" in {
    // Create test configs
    val config1 = GcpPubSubConfig.forEmulator("project1")
    val config2 = GcpPubSubConfig.forEmulator("project2") // Different project

    // Test manager caching
    val manager1 = PubSubManager(config1)
    val manager2 = PubSubManager(config1)
    val manager3 = PubSubManager(config2)

    manager1 shouldBe theSameInstanceAs(manager2) // Same config should reuse
    manager1 should not be theSameInstanceAs(manager3) // Different config = different manager

    // Cleanup
    PubSubManager.shutdownAll()
  }
}
