package ai.chronon.orchestration.temporal.workflow

import ai.chronon.orchestration.temporal.NodeExecutionRequest
import ai.chronon.orchestration.utils.{FuncUtils, TemporalUtils}
import ai.chronon.orchestration.temporal.constants.{
  NodeRangeCoordinatorWorkflowTaskQueue,
  NodeSingleStepWorkflowTaskQueue
}
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations.workflowRunTimeout
import io.temporal.activity.ActivityOptions
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture

/** Operations for interacting with Temporal workflows in the orchestration system.
  *
  * It supports the following operations:
  * 1. Start workflows for node processing
  * 2. Query workflow execution status
  * 3. Wait for workflow results
  */
class WorkflowOperations(workflowClient: WorkflowClient) {

  private val logger = LoggerFactory.getLogger(getClass)

  def startNodeSingleStepWorkflow(nodeExecutionRequest: NodeExecutionRequest): CompletableFuture[Void] = {
    val workflowId =
      TemporalUtils.getNodeSingleStepWorkflowId(nodeExecutionRequest)

    // Already existing workflow run so just wait for it instead
    try {
      if (getWorkflowStatus(workflowId) == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
        return getWorkflowResult(workflowId)
      }
    } catch {
      case e: Exception =>
        logger.info(s"No running workflow for ${nodeExecutionRequest} so starting a new one")
    }

    val workflowOptions = WorkflowOptions
      .newBuilder()
      .setWorkflowId(workflowId)
      .setTaskQueue(NodeSingleStepWorkflowTaskQueue.toString)
      .setWorkflowRunTimeout(workflowRunTimeout)
      .build()

    val workflow = workflowClient.newWorkflowStub(classOf[NodeSingleStepWorkflow], workflowOptions)
    WorkflowClient.start(FuncUtils.toTemporalProc(workflow.runSingleNodeStep(nodeExecutionRequest)))

    val workflowStub = workflowClient.newUntypedWorkflowStub(workflowId)
    workflowStub.getResultAsync(classOf[Void])
  }

  def startNodeRangeCoordinatorWorkflow(nodeExecutionRequest: NodeExecutionRequest): CompletableFuture[Void] = {
    val workflowId =
      TemporalUtils.getNodeRangeCoordinatorWorkflowId(nodeExecutionRequest)

    val workflowOptions = WorkflowOptions
      .newBuilder()
      .setWorkflowId(workflowId)
      .setTaskQueue(NodeRangeCoordinatorWorkflowTaskQueue.toString)
      .setWorkflowRunTimeout(workflowRunTimeout)
      .build()

    val workflow = workflowClient.newWorkflowStub(classOf[NodeRangeCoordinatorWorkflow], workflowOptions)
    WorkflowClient.start(FuncUtils.toTemporalProc(workflow.coordinateNodeRange(nodeExecutionRequest)))

    val workflowStub = workflowClient.newUntypedWorkflowStub(workflowId)
    workflowStub.getResultAsync(classOf[Void])
  }

  def getWorkflowStatus(workflowId: String): WorkflowExecutionStatus = {
    val describeWorkflowResp = workflowClient.getWorkflowServiceStubs
      .blockingStub()
      .describeWorkflowExecution(
        DescribeWorkflowExecutionRequest
          .newBuilder()
          .setNamespace(workflowClient.getOptions.getNamespace)
          .setExecution(
            WorkflowExecution
              .newBuilder()
              .setWorkflowId(workflowId)
              .build()
          )
          .build()
      )
    describeWorkflowResp.getWorkflowExecutionInfo.getStatus
  }

  def getWorkflowResult(workflowId: String): CompletableFuture[Void] = {
    val workflowStub = workflowClient.newUntypedWorkflowStub(workflowId)
    workflowStub.getResultAsync(classOf[Void])
  }

  def getWorkflowResult(workflowId: String, runId: String): CompletableFuture[Void] = {
    val workflowStub = workflowClient.newUntypedWorkflowStub(workflowId, Optional.of(runId), Optional.empty())
    workflowStub.getResultAsync(classOf[Void])
  }
}

object WorkflowOperations {
  // TODO: To pull these values from the node execution info thrift object
  val workflowRunTimeout: Duration = Duration.ofDays(1)

  // TODO: To make the activity options configurable
  val activityOptions: ActivityOptions = ActivityOptions
    .newBuilder()
    .setStartToCloseTimeout(Duration.ofHours(6))
    .build()
}
