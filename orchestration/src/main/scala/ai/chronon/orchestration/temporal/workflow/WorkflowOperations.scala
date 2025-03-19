package ai.chronon.orchestration.temporal.workflow

import ai.chronon.orchestration.utils.FuncUtils
import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.constants.{NodeExecutionWorkflowTaskQueue, TaskQueue}
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.client.{WorkflowClient, WorkflowOptions}

import java.time.Duration
import java.util.concurrent.CompletableFuture

// Interface for workflow operations
trait WorkflowOperations {
  def startNodeWorkflow(node: DummyNode): CompletableFuture[Void]

  def getWorkflowStatus(workflowId: String): WorkflowExecutionStatus
}

// Implementation using WorkflowClient
class WorkflowOperationsImpl(workflowClient: WorkflowClient) extends WorkflowOperations {

  override def startNodeWorkflow(node: DummyNode): CompletableFuture[Void] = {
    val workflowId = s"node-execution-${node.getName}"

    val workflowOptions = WorkflowOptions
      .newBuilder()
      .setWorkflowId(workflowId)
      .setTaskQueue(NodeExecutionWorkflowTaskQueue.toString)
      .setWorkflowRunTimeout(Duration.ofHours(1))
      .build()

    val workflow = workflowClient.newWorkflowStub(classOf[NodeExecutionWorkflow], workflowOptions)
    WorkflowClient.start(FuncUtils.toTemporalProc(workflow.executeNode(node)))

    val workflowStub = workflowClient.newUntypedWorkflowStub(workflowId)
    workflowStub.getResultAsync(classOf[Void])
  }

  override def getWorkflowStatus(workflowId: String): WorkflowExecutionStatus = {
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
}
