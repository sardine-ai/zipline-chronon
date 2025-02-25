package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.PhysicalNodeGraph
import ai.chronon.orchestration.temporal.constants.DAGExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftDataConverter
import ai.chronon.orchestration.temporal.workflow.{DAGExecutionWorkflow, DAGExecutionWorkflowImpl}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import io.temporal.client.{WorkflowClient, WorkflowClientOptions, WorkflowOptions}
import io.temporal.testing.{TestEnvironmentOptions, TestWorkflowEnvironment}
import io.temporal.worker.Worker

class DAGExecutionWorkflowTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var thriftDataConverter: ThriftDataConverter = new ThriftDataConverter

  override def beforeEach(): Unit = {
    val clientOptions = WorkflowClientOptions
      .newBuilder()
      .setNamespace("test-namespace")
      .setIdentity("test-identity")
      .setDataConverter(thriftDataConverter)
      .build()
    val testEnvOptions = TestEnvironmentOptions
      .newBuilder()
      .setWorkflowClientOptions(clientOptions)
      .build()
    testEnv = TestWorkflowEnvironment.newInstance(testEnvOptions)
    worker = testEnv.newWorker(DAGExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[DAGExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  // TODO: Add more unit tests around failure scenarios
  "DAGExecutionWorkflow" should "execute dag" in {
    testEnv.start()

    val options = WorkflowOptions
      .newBuilder()
      .setTaskQueue(DAGExecutionWorkflowTaskQueue.toString)
      .build()

    val workflow = workflowClient.newWorkflowStub(classOf[DAGExecutionWorkflow], options)
    val dag = new PhysicalNodeGraph()
    workflow.execute(dag)
    // TODO: Add more checks after workflow implementation
  }
}
