package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import io.temporal.activity.{Activity, ActivityInterface, ActivityMethod}

/** Defines helper activity methods that are needed for node execution workflow
  */
@ActivityInterface trait NodeExecutionActivity {

  /** Does one of the following steps in order for the node dependency
    *  1. Progress further for already completed node dependency workflow by reading from storage
    *  2. Wait for currently running node dependency workflow if it's already triggered
    *  3. Trigger a new node dependency workflow run
    */
  @ActivityMethod def triggerDependency(dependency: DummyNode): Unit

  // Submits the job for the node to the agent when the dependencies are met
  @ActivityMethod def submitJob(node: DummyNode): Unit
}

/** Dependency injection through constructor is supported for activities but not for workflows
  * https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeExecutionActivityImpl(workflowOps: WorkflowOperations) extends NodeExecutionActivity {

  override def triggerDependency(dependency: DummyNode): Unit = {

    val context = Activity.getExecutionContext
    context.doNotCompleteOnReturn()

    // This is needed as we don't want to finish the activity task till the async node workflow for the dependency
    // is complete.
    val completionClient = context.useLocalManualCompletion()

    // TODO: To properly cover all three cases as mentioned in the above interface definition
    val future = workflowOps.startNodeWorkflow(dependency)

    future.whenComplete((result, error) => {
      if (error != null) {
        completionClient.fail(error)
      } else {
        completionClient.complete(result)
      }
    })
  }

  override def submitJob(node: DummyNode): Unit = {
    // TODO: Actual Implementation for job submission
  }
}
