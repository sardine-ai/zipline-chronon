package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.persistence.NodeTableDependency
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityImpl
import ai.chronon.orchestration.temporal.constants.NodeSingleStepWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.{NodeSingleStepWorkflow, NodeSingleStepWorkflowImpl}
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import ai.chronon.orchestration.test.utils.TestUtils._
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration

class NodeSingleStepWorkflowSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeSingleStepWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val testBranch = Branch("test")

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var nodeSingleStepWorkflow: NodeSingleStepWorkflow = _
  private val mockNodeExecutionActivity: NodeExecutionActivityImpl = mock[NodeExecutionActivityImpl]

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeSingleStepWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeSingleStepWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Register the mock activity with worker
    worker.registerActivitiesImplementations(mockNodeExecutionActivity)

    // Start the test environment
    testEnv.start()

    // Create node execution workflow after starting test environment
    nodeSingleStepWorkflow = workflowClient.newWorkflowStub(classOf[NodeSingleStepWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "trigger all necessary activities with table dependencies" in {
    val rootNode = NodeName("root")
    val nodeExecutionRequest = NodeExecutionRequest(
      rootNode,
      testBranch,
      PartitionRange("2023-01-02", "2023-01-31")
    )

    // Create test table dependencies
    val dependencies = Seq(
      NodeTableDependency(
        rootNode,
        NodeName("dep1"),
        createTestTableDependency("test_table_1", Some("dt"), Some(1)) // With 1-day start offset
      ),
      NodeTableDependency(
        rootNode,
        NodeName("dep2"),
        createTestTableDependency("test_table_2", None, Some(0), Some(1)) // With 1-day end offset
      )
    )

    // Mock the activity method calls
    when(mockNodeExecutionActivity.getTableDependencies(nodeExecutionRequest.nodeName))
      .thenReturn(dependencies)

    // Execute the workflow
    nodeSingleStepWorkflow.runSingleNodeStep(nodeExecutionRequest)

    // Verify table dependencies are retrieved
    verify(mockNodeExecutionActivity).getTableDependencies(nodeExecutionRequest.nodeName)

    // Create argument captor to inspect triggerDependency calls
    val requestCaptor = ArgumentCaptor.forClass(classOf[NodeExecutionRequest])

    // Verify triggerDependency was called twice (once for each dependency)
    verify(mockNodeExecutionActivity, Mockito.times(2))
      .triggerDependency(requestCaptor.capture())

    // Get the captured arguments
    val capturedRequests = requestCaptor.getAllValues

    // Verify the node names match our dependencies
    val capturedNodeNames = capturedRequests.toScala
      .map(request => request.nodeName.name)
      .toSeq

    capturedNodeNames should contain theSameElementsAs Seq("dep1", "dep2")

    // Check that the partition ranges were computed correctly based on offsets
    capturedRequests.forEach { request =>
      if (request.nodeName.name == "dep2") {
        // The dep2 had a 1-day end offset
        request.partitionRange.start should be("2023-01-02")
        request.partitionRange.end should be("2023-01-30")
      } else {
        // The dep1 had 1-day start offset
        request.partitionRange.start should be("2023-01-01")
        request.partitionRange.end should be("2023-01-31")
      }
    }

    // Verify job submission
    verify(mockNodeExecutionActivity).submitJob(nodeExecutionRequest.nodeName)

    // Verify node run registration and status updates
    verify(mockNodeExecutionActivity).registerNodeRun(ArgumentMatchers.any())
    verify(mockNodeExecutionActivity).updateNodeRunStatus(ArgumentMatchers.any())
  }
}
