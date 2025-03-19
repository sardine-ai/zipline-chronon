package ai.chronon.orchestration.test.temporal.activity

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.activity.{NodeExecutionActivity, NodeExecutionActivityImpl}
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import io.temporal.activity.ActivityOptions
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.lang.{Void => JavaVoid}
import java.time.Duration
import java.util.concurrent.CompletableFuture

// Test workflow just for activity testing
// This is needed for testing manual completion logic for our activity as it's not supported for
// test activity environment
@WorkflowInterface
trait TestActivityWorkflow {
  @WorkflowMethod
  def triggerDependency(node: DummyNode): Unit
}

class TestActivityWorkflowImpl extends TestActivityWorkflow {
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
  private var testActivityWorkflow: TestActivityWorkflow = _

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[TestActivityWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Create mock workflow operations
    mockWorkflowOps = mock[WorkflowOperations]

    // Create activity with mocked dependencies
    val activity = new NodeExecutionActivityImpl(mockWorkflowOps)
    worker.registerActivitiesImplementations(activity)

    // Start the test environment
    testEnv.start()

    // Create test activity workflow
    testActivityWorkflow = workflowClient.newWorkflowStub(classOf[TestActivityWorkflow], workflowOptions)
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
    testActivityWorkflow.triggerDependency(testNode)

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
      testActivityWorkflow.triggerDependency(testNode)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the mocked method was called
    verify(mockWorkflowOps, atLeastOnce()).startNodeWorkflow(testNode)
  }

  it should "submit job successfully" in {
    val testActivityEnvironment = TemporalTestEnvironmentUtils.getTestActivityEnv

    // Get the activity stub (interface) to use for testing
    val activity = testActivityEnvironment.newActivityStub(
      classOf[NodeExecutionActivity],
      ActivityOptions
        .newBuilder()
        .setScheduleToCloseTimeout(Duration.ofSeconds(10))
        .build()
    )

    // Create activity implementation with mock workflow operations
    val activityImpl = new NodeExecutionActivityImpl(mockWorkflowOps)

    // Register activity implementation with the test environment
    testActivityEnvironment.registerActivitiesImplementations(activityImpl)

    val testNode = new DummyNode().setName("test-node")

    activity.submitJob(testNode)
    testActivityEnvironment.close()
  }
}
