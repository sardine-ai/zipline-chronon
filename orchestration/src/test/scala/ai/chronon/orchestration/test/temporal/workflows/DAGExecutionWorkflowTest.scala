package ai.chronon.orchestration.test.temporal.workflows

import ai.chronon.orchestration.PhysicalNodeGraph
import ai.chronon.orchestration.temporal.constants.DAGExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflows.{DAGExecutionWorkflow, DAGExecutionWorkflowImpl}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker

class DAGExecutionWorkflowTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _

  override def beforeEach(): Unit = {
    testEnv = TestWorkflowEnvironment.newInstance()
    worker = testEnv.newWorker(DAGExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[DAGExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  "DAGExecutionWorkflow" should "execute dag" in {
    testEnv.start()

    val options = WorkflowOptions
      .newBuilder()
      .setTaskQueue(DAGExecutionWorkflowTaskQueue.toString)
      .build()

    val workflow = workflowClient.newWorkflowStub(classOf[DAGExecutionWorkflow], options)
    val dag = new PhysicalNodeGraph()
    dag.setName("wf")
    workflow.execute(dag)
  }
}
