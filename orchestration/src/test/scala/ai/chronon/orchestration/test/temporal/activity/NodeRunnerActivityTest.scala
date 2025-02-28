package ai.chronon.orchestration.test.temporal.activity

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.activity.{NodeRunnerActivity, NodeRunnerActivityImpl}
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.storage.DependencyStorage
import ai.chronon.orchestration.temporal.workflow.{
  NodeExecutionWorkflowImpl,
  WorkflowOperations,
  WorkflowOperationsImpl
}
import io.temporal.activity.ActivityOptions
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.{WorkflowClient, WorkflowClientOptions, WorkflowOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.testing.{TestEnvironmentOptions, TestWorkflowEnvironment}
import io.temporal.worker.Worker
import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.collection.mutable
import java.util

// Test workflow just for activity testing
@WorkflowInterface
trait TestActivityWorkflow {
  @WorkflowMethod
  def executeActivity(node: DummyNode): Unit
}

class TestActivityWorkflowImpl extends TestActivityWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeRunnerActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .build()
  )

  override def executeActivity(node: DummyNode): Unit = {
    activity.triggerAndWaitForDependencies(node)
  }
}

class NodeRunnerActivityTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  /** We still go through all the following payload converters in the following order as specified below
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/DefaultDataConverter.java#L38
    * which is important for using other Types serialized using other payload converters, but we will be
    * overriding ByteArrayPayloadConverter with our custom ThriftPayloadConverter based on EncodingType
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/ByteArrayPayloadConverter.java#L30
    */
  private val customDataConverter = DefaultDataConverter.newDefaultInstance.withPayloadConverterOverrides(
    new ThriftPayloadConverter
  )
  private val clientOptions = WorkflowClientOptions
    .newBuilder()
    .setNamespace("test-namespace")
    .setIdentity("test-identity")
    .setDataConverter(customDataConverter)
    .build()
  private val testEnvOptions = TestEnvironmentOptions
    .newBuilder()
    .setWorkflowClientOptions(clientOptions)
    .build()
  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeExecutionWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  // Mock storage
  private val mockStorage = new DependencyStorage {
    private val completedDeps = mutable.Set[String]()

    override def isDependencyCompleted(nodeId: String): Boolean =
      completedDeps.contains(nodeId)

    override def markDependencyCompleted(nodeId: String): Unit =
      completedDeps.add(nodeId)
  }

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var mockWorkflowOps: WorkflowOperations = _
  private var testActivityWorkflow: TestActivityWorkflow = _

  override def beforeEach(): Unit = {
    testEnv = TestWorkflowEnvironment.newInstance(testEnvOptions)
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[TestActivityWorkflowImpl], classOf[NodeExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Mock workflow operations
    mockWorkflowOps = new WorkflowOperationsImpl(workflowClient, NodeExecutionWorkflowTaskQueue.toString)

    // Create activity with mocked dependencies
    val activity = new NodeRunnerActivityImpl(mockWorkflowOps, mockStorage)
    worker.registerActivitiesImplementations(activity)

    // Start the test environment
    testEnv.start()

    // Create test activity workflow
    testActivityWorkflow = workflowClient.newWorkflowStub(classOf[TestActivityWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  // Create simple dependency graph:
  //          main
  //        /      \
  //     dep1      dep2
  private def getSimpleNode: DummyNode = {
    val depNode1 = new DummyNode().setName("dep1") // Leaf node 1
    val depNode2 = new DummyNode().setName("dep2") // Leaf node 2
    new DummyNode()
      .setName("main") // Root node
      .setDependencies(util.Arrays.asList(depNode1, depNode2)) // Main node depends on both leaf nodes
  }

  it should "handle simple node with one level deep correctly" in {
    // Trigger activity
    testActivityWorkflow.executeActivity(getSimpleNode)

    // Verify that all dependent node workflows are started and finished successfully
    for (dependentNode <- Array("dep1", "dep2")) {
      mockWorkflowOps.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }

  // Create complex dependency graph:
  //                    Derivation
  //                        |
  //                      Join
  //                   /        \
  //             GroupBy1     GroupBy2
  //                |            |
  //          StagingQuery1  StagingQuery2
  private def getComplexNode: DummyNode = {
    // Create base level queries (leaf nodes)
    val stagingQuery1 = new DummyNode().setName("StagingQuery1") // e.g., "SELECT * FROM raw_events"
    val stagingQuery2 = new DummyNode().setName("StagingQuery2") // e.g., "SELECT * FROM raw_metrics"

    // Create aggregation level nodes
    val groupBy1 = new DummyNode()
      .setName("GroupBy1") // e.g., "GROUP BY event_type, timestamp"
      .setDependencies(util.Arrays.asList(stagingQuery1))

    val groupBy2 = new DummyNode()
      .setName("GroupBy2") // e.g., "GROUP BY metric_name, interval"
      .setDependencies(util.Arrays.asList(stagingQuery2))

    // Create join level
    val join = new DummyNode()
      .setName("Join") // e.g., "JOIN grouped_events WITH grouped_metrics"
      .setDependencies(util.Arrays.asList(groupBy1, groupBy2))

    // Return final derivation node
    new DummyNode()
      .setName("Derivation") // e.g., "CALCULATE final_metrics"
      .setDependencies(util.Arrays.asList(join))
  }

  it should "handle complex node with multiple levels deep correctly" in {
    // Trigger activity
    testActivityWorkflow.executeActivity(getComplexNode)

    // Verify that all dependent node workflows are started and finished successfully
    // Activity for Derivation node should trigger all downstream node workflows
    for (dependentNode <- Array("StagingQuery1", "StagingQuery2", "GroupBy1", "GroupBy2", "Join")) {
      mockWorkflowOps.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }
}
