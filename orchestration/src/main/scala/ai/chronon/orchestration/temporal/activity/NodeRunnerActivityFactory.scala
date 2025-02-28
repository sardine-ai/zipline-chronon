package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.temporal.storage.DependencyStorage
import ai.chronon.orchestration.temporal.workflow.WorkflowOperationsImpl
import io.temporal.client.WorkflowClient

// Factory for creating activity implementations
object NodeRunnerActivityFactory {
  def create(
      workflowClient: WorkflowClient,
      taskQueue: String,
      storage: DependencyStorage
  ): NodeRunnerActivity = {
    val workflowOps = new WorkflowOperationsImpl(workflowClient, taskQueue)
    new NodeRunnerActivityImpl(workflowOps, storage)
  }
}
