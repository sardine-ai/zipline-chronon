package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityImpl
import ai.chronon.orchestration.temporal.constants.NodeRangeCoordinatorWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.{NodeRangeCoordinatorWorkflow, NodeRangeCoordinatorWorkflowImpl}
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration

class NodeRangeCoordinatorWorkflowSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeRangeCoordinatorWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val testBranch = Branch("test")

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var nodeRangeCoordinatorWorkflow: NodeRangeCoordinatorWorkflow = _
  private val mockNodeExecutionActivity: NodeExecutionActivityImpl = mock[NodeExecutionActivityImpl]

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeRangeCoordinatorWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeRangeCoordinatorWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Register the mock activity with worker
    worker.registerActivitiesImplementations(mockNodeExecutionActivity)

    // Start the test environment
    testEnv.start()

    // Create node execution workflow after starting test environment
    nodeRangeCoordinatorWorkflow =
      workflowClient.newWorkflowStub(classOf[NodeRangeCoordinatorWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "trigger all necessary activities" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("root"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-31")
    )

    // Mock the activity method calls
    when(mockNodeExecutionActivity.getMissingSteps(nodeExecutionRequest))
      .thenReturn(Seq(nodeExecutionRequest.partitionRange))

    // Execute the workflow
    nodeRangeCoordinatorWorkflow.coordinateNodeRange(nodeExecutionRequest)

    // Verify getMissingSteps was called
    verify(mockNodeExecutionActivity).getMissingSteps(nodeExecutionRequest)

    // Verify triggerMissingSteps activity call
    verify(mockNodeExecutionActivity).triggerMissingNodeSteps(nodeExecutionRequest.nodeName,
                                                              nodeExecutionRequest.branch,
                                                              Seq(nodeExecutionRequest.partitionRange))
  }
}
