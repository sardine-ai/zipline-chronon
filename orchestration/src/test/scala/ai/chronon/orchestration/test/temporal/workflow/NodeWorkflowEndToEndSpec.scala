package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.persistence.{NodeDao, NodeRun}
import ai.chronon.orchestration.pubsub.{PubSubMessage, PubSubPublisher}
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityImpl
import ai.chronon.orchestration.temporal.constants.{
  NodeRangeCoordinatorWorkflowTaskQueue,
  NodeSingleStepWorkflowTaskQueue
}
import ai.chronon.orchestration.temporal.workflow.{
  NodeRangeCoordinatorWorkflowImpl,
  NodeSingleStepWorkflowImpl,
  WorkflowOperations
}
import ai.chronon.orchestration.test.utils.{TemporalTestEnvironmentUtils, TestUtils}
import ai.chronon.orchestration.utils.TemporalUtils
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
import scala.concurrent.Future

class NodeWorkflowEndToEndSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val testBranch = Branch("test")

  private var testEnv: TestWorkflowEnvironment = _
  private var worker1: Worker = _
  private var worker2: Worker = _
  private var workflowClient: WorkflowClient = _
  private var mockPublisher: PubSubPublisher = _
  private var mockWorkflowOps: WorkflowOperations = _
  private var mockNodeDao: NodeDao = _

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    // Setup up workers
    worker1 = testEnv.newWorker(NodeRangeCoordinatorWorkflowTaskQueue.toString)
    worker1.registerWorkflowImplementationTypes(classOf[NodeRangeCoordinatorWorkflowImpl])
    worker2 = testEnv.newWorker(NodeSingleStepWorkflowTaskQueue.toString)
    worker2.registerWorkflowImplementationTypes(classOf[NodeSingleStepWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Mock workflow operations
    mockWorkflowOps = new WorkflowOperations(workflowClient)

    // Mock NodeDao
    mockNodeDao = mock[NodeDao]
    setupMockDependencies()

    // Mock PubSub publisher
    mockPublisher = mock[PubSubPublisher]
    val completedFuture = CompletableFuture.completedFuture("message-id-123")
    when(mockPublisher.publish(ArgumentMatchers.any[PubSubMessage])).thenReturn(completedFuture)
    when(mockPublisher.topicId).thenReturn("test-topic")

    // Create activity with mocked dependencies
    val activity = new NodeExecutionActivityImpl(mockWorkflowOps, mockNodeDao, mockPublisher)
    worker1.registerActivitiesImplementations(activity)
    worker2.registerActivitiesImplementations(activity)

    // Start the test environment
    testEnv.start()
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  // Helper method to set up mock dependencies for our DAG tests
  private def setupMockDependencies(): Unit = {
    // Simple node dependencies
    val rootDeps = Seq(
      TestUtils.createTestNodeTableDependency("root", "dep1", "root_to_dep1_table"),
      TestUtils.createTestNodeTableDependency("root", "dep2", "root_to_dep2_table")
    )
    when(mockNodeDao.getNodeTableDependencies(NodeName("root")))
      .thenReturn(Future.successful(rootDeps))
    when(mockNodeDao.getNodeTableDependencies(NodeName("dep1")))
      .thenReturn(Future.successful(Seq.empty))
    when(mockNodeDao.getNodeTableDependencies(NodeName("dep2")))
      .thenReturn(Future.successful(Seq.empty))

    // Complex node dependencies
    when(mockNodeDao.getNodeTableDependencies(NodeName("derivation")))
      .thenReturn(
        Future.successful(
          Seq(
            TestUtils.createTestNodeTableDependency("derivation", "join", "derivation_to_join_table")
          )))
    when(mockNodeDao.getNodeTableDependencies(NodeName("join")))
      .thenReturn(
        Future.successful(Seq(
          TestUtils.createTestNodeTableDependency("join", "groupBy1", "join_to_groupBy1_table"),
          TestUtils.createTestNodeTableDependency("join", "groupBy2", "join_to_groupBy2_table")
        )))
    when(mockNodeDao.getNodeTableDependencies(NodeName("groupBy1")))
      .thenReturn(
        Future.successful(
          Seq(
            TestUtils.createTestNodeTableDependency("groupBy1", "stagingQuery1", "groupBy1_to_stagingQuery1_table")
          )))
    when(mockNodeDao.getNodeTableDependencies(NodeName("groupBy2")))
      .thenReturn(
        Future.successful(
          Seq(
            TestUtils.createTestNodeTableDependency("groupBy2", "stagingQuery2", "groupBy2_to_stagingQuery2_table")
          )))
    when(mockNodeDao.getNodeTableDependencies(NodeName("stagingQuery1")))
      .thenReturn(Future.successful(Seq.empty))
    when(mockNodeDao.getNodeTableDependencies(NodeName("stagingQuery2")))
      .thenReturn(Future.successful(Seq.empty))

    // Mock node run dao functions
    when(mockNodeDao.findLatestCoveringRun(ArgumentMatchers.any[NodeExecutionRequest]))
      .thenReturn(Future.successful(None))
    when(mockNodeDao.insertNodeRun(ArgumentMatchers.any[NodeRun])).thenReturn(Future.successful(1))
    when(mockNodeDao.updateNodeRunStatus(ArgumentMatchers.any[NodeRun])).thenReturn(Future.successful(1))
    when(mockNodeDao.findOverlappingNodeRuns(ArgumentMatchers.any[NodeExecutionRequest]))
      .thenReturn(Future.successful(Seq.empty))
    when(mockNodeDao.getStepDays(ArgumentMatchers.any[NodeName]))
      .thenReturn(Future.successful(StepDays(1)))
  }

  private def verifyAllNodeWorkflows(nodeRangeCoordinatorRequests: Seq[NodeExecutionRequest],
                                     nodeSingleStepRequests: Seq[NodeExecutionRequest]): Unit = {
    // Verify that all node range coordinator workflows are started and finished successfully
    for (nodeRangeCoordinatorRequest <- nodeRangeCoordinatorRequests) {
      val workflowId = TemporalUtils.getNodeRangeCoordinatorWorkflowId(nodeRangeCoordinatorRequest)
      mockWorkflowOps.getWorkflowStatus(workflowId) should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }

    // Verify that all node step workflows are started and finished successfully
    for (nodeSingleStepRequest <- nodeSingleStepRequests) {
      val workflowId = TemporalUtils.getNodeSingleStepWorkflowId(nodeSingleStepRequest)
      mockWorkflowOps.getWorkflowStatus(workflowId) should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }

  it should "handle simple node with one level deep correctly" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("root"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-02")
    )

    // Trigger workflow and wait for it to complete
    mockWorkflowOps.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest).get()

    val nodeRangeCoordinatorRequests = Seq(
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-01", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    val nodeSingleStepRequests = Seq(
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    // Verify that all node workflows are started and finished successfully
    verifyAllNodeWorkflows(nodeRangeCoordinatorRequests, nodeSingleStepRequests)
  }

  it should "handle complex node with multiple levels deep correctly" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("derivation"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-02")
    )
    // Trigger workflow and wait for it to complete
    mockWorkflowOps.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest).get()

    // Define the expected workflows that should be executed
    val nodeRangeCoordinatorRequests = Seq(
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-01", "2023-01-02")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    val nodeSingleStepRequests = Seq(
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    // Verify that all node workflows are started and finished successfully
    verifyAllNodeWorkflows(nodeRangeCoordinatorRequests, nodeSingleStepRequests)
  }
}
