package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityFactory
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.workflow.{
  NodeExecutionWorkflowImpl,
  WorkflowOperations,
  WorkflowOperationsImpl
}
import ai.chronon.orchestration.test.utils.{TemporalTestEnvironmentUtils, TestNodeUtils}
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.{WorkflowClient, WorkflowClientOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.WorkerFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** This will trigger workflow runs on the local temporal server, so the pre-requisite would be to have the
  * temporal service running locally using `temporal server start-dev`
  */
class NodeExecutionWorkflowIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var workflowClient: WorkflowClient = _
  private var workflowOperations: WorkflowOperations = _
  private var factory: WorkerFactory = _

  override def beforeAll(): Unit = {
    workflowClient = TemporalTestEnvironmentUtils.getLocalWorkflowClient

    workflowOperations = new WorkflowOperationsImpl(workflowClient)

    factory = WorkerFactory.newInstance(workflowClient)

    // Setup worker for node workflow execution
    val worker = factory.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])
    worker.registerActivitiesImplementations(NodeExecutionActivityFactory.create(workflowClient))

    // Start all registered Workers. The Workers will start polling the Task Queue.
    factory.start()
  }

  override def afterAll(): Unit = {
    factory.shutdown()
  }

  it should "handle simple node with one level deep correctly" in {
    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeWorkflow(TestNodeUtils.getSimpleNode).get()

    // Verify that all node workflows are started and finished successfully
    for (dependentNode <- Array("dep1", "dep2", "main")) {
      workflowOperations.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }

  it should "handle complex node with multiple levels deep correctly" in {
    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeWorkflow(TestNodeUtils.getComplexNode).get()

    // Verify that all dependent node workflows are started and finished successfully
    // Activity for Derivation node should trigger all downstream node workflows
    for (dependentNode <- Array("StagingQuery1", "StagingQuery2", "GroupBy1", "GroupBy2", "Join", "Derivation")) {
      workflowOperations.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }
  }
}
