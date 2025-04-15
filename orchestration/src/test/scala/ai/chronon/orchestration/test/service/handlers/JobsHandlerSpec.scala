package ai.chronon.orchestration.test.service.handlers

import ai.chronon.api.{Job, JobListGetRequest, JobListResponse}
import ai.chronon.orchestration.pubsub.{JobSubmissionMessage, PubSubManager, PubSubMessage, PubSubSubscriber}
import ai.chronon.orchestration.service.handlers.JobsHandler
import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._

/** Unit tests for the JobsHandler class */
class JobsHandlerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "JobsHandler.getJobs" should "return empty job list when no messages are available" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Set up mock to return empty message list
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenReturn(Seq.empty)

    // Create handler and call getJobs
    val handler = new JobsHandler(mockPubSubManager)
    val response = handler.getJobs(request)

    // Verify response has empty lists
    response.getJobsToStart should not be null
    response.getJobsToStart.size() shouldBe 0
    response.getJobsToStop should not be null
    response.getJobsToStop.size() shouldBe 0
  }

  it should "convert PubSub messages to Job objects" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Create test messages
    val message1 = createTestMessage("node-1", Map("attr1" -> "value1"))
    val message2 = createTestMessage("node-2", Map("attr2" -> "value2"))
    val messages = Seq(message1, message2)

    // Set up mock to return our test messages
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenReturn(messages)

    // Create handler and call getJobs
    val handler = new JobsHandler(mockPubSubManager)
    val response = handler.getJobs(request)

    // Verify jobs were created from messages
    response.getJobsToStart.size() shouldBe 2

    // Verify job content
    val jobs = response.getJobsToStart.asScala.toSeq
    jobs.map(_.getJobId).toSet should contain allElementsOf Set("node-1", "node-2").map(name =>
      jobs.find(_.getJobId.startsWith(s"$name-")).get.getJobId)
  }

  it should "skip messages without nodeName attribute" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Create test messages - one with nodeName, one without
    val validMessage = createTestMessage("valid-node", Map("attr1" -> "value1"))
    val invalidMessage = createEmptyTestMessage()
    val messages = Seq(validMessage, invalidMessage)

    // Set up mock to return our test messages
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenReturn(messages)

    // Create handler and call getJobs
    val handler = new JobsHandler(mockPubSubManager)
    val response = handler.getJobs(request)

    // Verify only one job was created (from the valid message)
    response.getJobsToStart.size() shouldBe 1
    response.getJobsToStart.get(0).getJobId should startWith("valid-node-")
  }

  it should "throw exceptions when pulling messages fails" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Set up mock to throw exception when pulling messages
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenThrow(new RuntimeException("Error pulling messages"))

    // Create handler and call getJobs - should throw exception
    val handler = new JobsHandler(mockPubSubManager)

    // Verify the exception is propagated
    val exception = intercept[RuntimeException] {
      handler.getJobs(request)
    }

    // Verify the exception message
    exception.getMessage should include("Error pulling messages")
  }

  it should "throw exceptions when processing messages fails" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]
    val mockBadMessage = mock[PubSubMessage]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Set up mock to return our test message and throw exception when getting attributes
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenReturn(Seq(mockBadMessage))
    when(mockBadMessage.getAttributes).thenThrow(new RuntimeException("Error getting attributes"))

    // Create handler and call getJobs - should throw exception
    val handler = new JobsHandler(mockPubSubManager)

    // Verify the exception is propagated
    val exception = intercept[RuntimeException] {
      handler.getJobs(request)
    }

    // Verify the exception message
    exception.getMessage should include("Error getting attributes")
  }

  it should "use a unique subscription ID based on service name" in {
    // Mock dependencies
    val mockPubSubManager = mock[PubSubManager]
    val mockSubscriber = mock[PubSubSubscriber]

    // Mock request with a topic ID
    val request = new JobListGetRequest()
    request.setTopicId("test-topic")

    // Set up mock
    when(mockPubSubManager.getOrCreateSubscriber(anyString(), anyString())).thenReturn(mockSubscriber)
    when(mockSubscriber.pullMessages()).thenReturn(Seq.empty)

    val expectedSubscriptionId = "orchestration-service"

    // Create handler and call getJobs
    val handler = new JobsHandler(mockPubSubManager)
    handler.getJobs(request)

    // Verify the subscription ID was created with hostname
    verify(mockPubSubManager).getOrCreateSubscriber(
      eqTo("test-topic"),
      eqTo(expectedSubscriptionId)
    )
  }

  // Helper methods to create test messages
  private def createTestMessage(nodeName: String, attributes: Map[String, String]): PubSubMessage = {
    new PubSubMessage {
      override def getAttributes: Map[String, String] = attributes + ("nodeName" -> nodeName)
      override def getData: Option[Array[Byte]] = None
    }
  }

  private def createEmptyTestMessage(): PubSubMessage = {
    new PubSubMessage {
      override def getAttributes: Map[String, String] = Map.empty
      override def getData: Option[Array[Byte]] = None
    }
  }
}
