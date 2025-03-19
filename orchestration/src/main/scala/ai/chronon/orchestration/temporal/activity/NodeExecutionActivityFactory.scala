package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.temporal.workflow.WorkflowOperationsImpl
import io.temporal.client.WorkflowClient

// Factory for creating activity implementations
object NodeExecutionActivityFactory {
  def create(workflowClient: WorkflowClient): NodeExecutionActivity = {
    val workflowOps = new WorkflowOperationsImpl(workflowClient)
    new NodeExecutionActivityImpl(workflowOps)
  }
}
