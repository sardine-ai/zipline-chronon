package ai.chronon.orchestration.temporal.workflow

import ai.chronon.orchestration.temporal.NodeExecutionRequest
import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivity

/** Temporal workflow for coordinating node execution across a time range.
  *
  * This higher-level workflow coordinates the execution of a node across multiple
  * time partitions (steps). It's responsible for:
  *
  * 1. Analyzing the requested time range to identify partitions that need processing
  * 2. Breaking down the work into individual steps
  * 3. Coordinating execution of those steps, potentially in parallel
  * 4. Managing the overall completion of the entire partition range
  *
  * This approach allows for efficient processing of large time ranges by:
  * - Only processing partitions that haven't been completed
  * - Maximizing parallelism where possible
  */
@WorkflowInterface
trait NodeRangeCoordinatorWorkflow {

  /** Coordinates the execution of a node across a date range.
    *
    * @param nodeExecutionRequest The request containing node name, branch, and date range
    * @return Unit, with success recorded via the individual step executions
    */
  @WorkflowMethod def coordinateNodeRange(nodeExecutionRequest: NodeExecutionRequest): Unit;
}

/** Note: Constructor-based dependency injection is not supported in Temporal workflows.
  * See: https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeRangeCoordinatorWorkflowImpl extends NodeRangeCoordinatorWorkflow {

  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    WorkflowOperations.activityOptions
  )

  override def coordinateNodeRange(nodeExecutionRequest: NodeExecutionRequest): Unit = {
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)
    activity.triggerMissingNodeSteps(nodeExecutionRequest.nodeName, nodeExecutionRequest.branch, missingSteps)
  }
}
