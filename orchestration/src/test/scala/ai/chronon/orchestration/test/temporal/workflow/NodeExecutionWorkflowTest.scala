package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityImpl
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.{NodeExecutionWorkflow, NodeExecutionWorkflowImpl}
import ai.chronon.orchestration.test.utils.{TemporalTestEnvironmentUtils, TestNodeUtils}
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.mockito.Mockito.verify
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration

class NodeExecutionWorkflowTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeExecutionWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var nodeExecutionWorkflow: NodeExecutionWorkflow = _
  private val mockNodeExecutionActivity: NodeExecutionActivityImpl = mock[NodeExecutionActivityImpl]

  override def beforeEach(): Unit = {
    testEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Register the mock activity with worker
    worker.registerActivitiesImplementations(mockNodeExecutionActivity)

    // Start the test environment
    testEnv.start()

    // Create node execution workflow after starting test environment
    nodeExecutionWorkflow = workflowClient.newWorkflowStub(classOf[NodeExecutionWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "trigger all necessary activities" in {
    val node = TestNodeUtils.getSimpleNode
    nodeExecutionWorkflow.executeNode(node)

    // Verify all dependencies are met
    for (dep <- node.dependencies.toScala) {
      verify(mockNodeExecutionActivity).triggerDependency(dep)
    }
    // Verify job submission
    verify(mockNodeExecutionActivity).submitJob(node)
  }
}
