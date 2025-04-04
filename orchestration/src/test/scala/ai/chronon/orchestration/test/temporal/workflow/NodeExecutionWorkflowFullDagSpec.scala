package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.pubsub.{GcpPubSubMessage, PubSubMessage, PubSubPublisher}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityImpl
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.{
  NodeExecutionWorkflowImpl,
  WorkflowOperations,
  WorkflowOperationsImpl
}
import ai.chronon.orchestration.test.utils.{TemporalTestEnvironmentUtils, TestNodeUtils}
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.WorkflowClient
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.concurrent.CompletableFuture

class NodeExecutionWorkflowFullDagSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var mockPublisher: PubSubPublisher = _
  private var mockWorkflowOps: WorkflowOperations = _

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Mock workflow operations
    mockWorkflowOps = new WorkflowOperationsImpl(workflowClient)

    // Mock PubSub publisher
    mockPublisher = mock[PubSubPublisher]
    val completedFuture = CompletableFuture.completedFuture("message-id-123")
    when(mockPublisher.publish(ArgumentMatchers.any[PubSubMessage])).thenReturn(completedFuture)
    when(mockPublisher.topicId).thenReturn("test-topic")

    // Create activity with mocked dependencies
    val activity = new NodeExecutionActivityImpl(mockWorkflowOps, mockPublisher)
    worker.registerActivitiesImplementations(activity)

    // Start the test environment
    testEnv.start()
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "handle simple node with one level deep correctly" in {
    // Trigger workflow and wait for it to complete
    mockWorkflowOps.startNodeWorkflow(TestNodeUtils.getSimpleNode).get()

    // Verify that all node workflows are started and finished successfully
    for (dependentNode <- Array("dep1", "dep2", "main")) {
      mockWorkflowOps.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }

  it should "handle complex node with multiple levels deep correctly" in {
    // Trigger workflow and wait for it to complete
    mockWorkflowOps.startNodeWorkflow(TestNodeUtils.getComplexNode).get()

    // Verify that all dependent node workflows are started and finished successfully
    // Activity for Derivation node should trigger all downstream node workflows
    for (dependentNode <- Array("StagingQuery1", "StagingQuery2", "GroupBy1", "GroupBy2", "Join", "Derivation")) {
      mockWorkflowOps.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }
}
