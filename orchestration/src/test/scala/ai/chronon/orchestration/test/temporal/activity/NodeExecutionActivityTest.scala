package ai.chronon.orchestration.test.temporal.activity

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.pubsub.{JobSubmissionMessage, PubSubPublisher}
import ai.chronon.orchestration.temporal.activity.{NodeExecutionActivity, NodeExecutionActivityImpl}
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import io.temporal.activity.ActivityOptions
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.lang.{Void => JavaVoid}
import java.time.Duration
import java.util.concurrent.CompletableFuture

// Test workflows for activity testing
// These are needed for testing manual completion logic for our activities as it's not supported for
// test activity environment

// Workflow for testing triggerDependency
@WorkflowInterface
trait TestTriggerDependencyWorkflow {
  @WorkflowMethod
  def triggerDependency(node: DummyNode): Unit
}

class TestTriggerDependencyWorkflowImpl extends TestTriggerDependencyWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .build()
  )

  override def triggerDependency(node: DummyNode): Unit = {
    activity.triggerDependency(node)
  }
}

// Workflow for testing submitJob
@WorkflowInterface
trait TestSubmitJobWorkflow {
  @WorkflowMethod
  def submitJob(node: DummyNode): Unit
}

class TestSubmitJobWorkflowImpl extends TestSubmitJobWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .build()
  )

  override def submitJob(node: DummyNode): Unit = {
    activity.submitJob(node)
  }
}

class NodeExecutionActivityTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with MockitoSugar {

  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeExecutionWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var mockWorkflowOps: WorkflowOperations = _
  private var mockPublisher: PubSubPublisher = _
  private var testTriggerWorkflow: TestTriggerDependencyWorkflow = _
  private var testSubmitWorkflow: TestSubmitJobWorkflow = _

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(
      classOf[TestTriggerDependencyWorkflowImpl],
      classOf[TestSubmitJobWorkflowImpl]
    )
    workflowClient = testEnv.getWorkflowClient

    // Create mock dependencies
    mockWorkflowOps = mock[WorkflowOperations]
    mockPublisher = mock[PubSubPublisher]
    when(mockPublisher.topicId).thenReturn("test-topic")

    // Create activity with mocked dependencies
    val activity = new NodeExecutionActivityImpl(mockWorkflowOps, mockPublisher)
    worker.registerActivitiesImplementations(activity)

    // Start the test environment
    testEnv.start()

    // Create test activity workflows
    testTriggerWorkflow = workflowClient.newWorkflowStub(classOf[TestTriggerDependencyWorkflow], workflowOptions)
    testSubmitWorkflow = workflowClient.newWorkflowStub(classOf[TestSubmitJobWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    if (testEnv != null) {
      testEnv.close()
    }
  }

  it should "trigger and successfully wait for activity completion" in {
    val testNode = new DummyNode().setName("test-node")
    val completedFuture = CompletableFuture.completedFuture[JavaVoid](null)

    // Mock workflow operations
    when(mockWorkflowOps.startNodeWorkflow(testNode)).thenReturn(completedFuture)

    // Trigger activity method
    testTriggerWorkflow.triggerDependency(testNode)

    // Assert
    verify(mockWorkflowOps).startNodeWorkflow(testNode)
  }

  it should "fail when the dependency workflow fails" in {
    val testNode = new DummyNode().setName("failing-node")
    val expectedException = new RuntimeException("Workflow execution failed")
    val failedFuture = new CompletableFuture[JavaVoid]()
    failedFuture.completeExceptionally(expectedException)

    // Mock workflow operations to return a failed future
    when(mockWorkflowOps.startNodeWorkflow(testNode)).thenReturn(failedFuture)

    // Trigger activity and expect it to fail
    val exception = intercept[RuntimeException] {
      testTriggerWorkflow.triggerDependency(testNode)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the mocked method was called
    verify(mockWorkflowOps, atLeastOnce()).startNodeWorkflow(testNode)
  }

  it should "submit job successfully" in {
    val testNode = new DummyNode().setName("test-node")
    val completedFuture = CompletableFuture.completedFuture("message-id-123")

    // Mock PubSub publisher to return a completed future
    when(mockPublisher.publish(ArgumentMatchers.any[JobSubmissionMessage])).thenReturn(completedFuture)

    // Trigger activity method
    testSubmitWorkflow.submitJob(testNode)

    // Use a capture to verify the message passed to the publisher
    val messageCaptor = ArgumentCaptor.forClass(classOf[JobSubmissionMessage])
    verify(mockPublisher).publish(messageCaptor.capture())

    // Verify the message content
    val capturedMessage = messageCaptor.getValue
    capturedMessage.nodeName should be(testNode.name)
  }

  it should "fail when publishing to PubSub fails" in {
    val testNode = new DummyNode().setName("failing-node")
    val expectedException = new RuntimeException("Failed to publish message")
    val failedFuture = new CompletableFuture[String]()
    failedFuture.completeExceptionally(expectedException)

    // Mock PubSub publisher to return a failed future
    when(mockPublisher.publish(ArgumentMatchers.any[JobSubmissionMessage])).thenReturn(failedFuture)

    // Trigger activity and expect it to fail
    val exception = intercept[RuntimeException] {
      testSubmitWorkflow.submitJob(testNode)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the message was passed to the publisher
    verify(mockPublisher, atLeastOnce()).publish(ArgumentMatchers.any[JobSubmissionMessage])
  }
}
