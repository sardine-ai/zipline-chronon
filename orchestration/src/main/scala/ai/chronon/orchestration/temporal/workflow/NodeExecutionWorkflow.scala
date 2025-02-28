package ai.chronon.orchestration.temporal.workflow

import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.activity.NodeRunnerActivity
import io.temporal.activity.ActivityOptions

import java.time.Duration

/** TODO: Add more documentation
  * Workflow for individual node execution with in a DAG
  */
@WorkflowInterface
trait NodeExecutionWorkflow {
  @WorkflowMethod def executeNode(node: DummyNode): Unit;
}

/** Dependency injection through constructor for workflows is not directly supported
  * https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeExecutionWorkflowImpl extends NodeExecutionWorkflow {

  private val activity = Workflow.newActivityStub(
    classOf[NodeRunnerActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofMinutes(10))
      .build()
  )

  override def executeNode(node: DummyNode): Unit = {
    // Use activity
    activity.triggerAndWaitForDependencies(node)
    activity.submitJob(node)
  }
}
