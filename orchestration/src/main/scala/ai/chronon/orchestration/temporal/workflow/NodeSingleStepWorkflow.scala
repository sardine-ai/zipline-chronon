package ai.chronon.orchestration.temporal.workflow

import ai.chronon.api.planner.DependencyResolver.computeInputRange
import ai.chronon.api.PartitionSpec
import ai.chronon.orchestration.NodeRunStatus
import ai.chronon.orchestration.persistence.NodeRun
import ai.chronon.orchestration.temporal.NodeExecutionRequest
import io.temporal.workflow.{Async, Promise, Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivity
import io.temporal.activity.ActivityOptions

import java.time.Duration

/** Temporal workflow for executing a single step of a data processing node.
  *
  * This workflow handles the execution of a single time partition (step) for a node in the
  * computation graph. It ensures that:
  * 1. Node execution is tracked with persistent state in the database
  * 2. All dependency nodes are executed first
  * 3. The node job is submitted only when all dependencies are satisfied
  * 4. The execution status is properly recorded
  */
@WorkflowInterface
trait NodeSingleStepWorkflow {

  /** Executes a single step for a node within a partition range.
    *
    * @param nodeExecutionRequest The request containing node name, branch, and partition range
    * @return Unit, with success/failure status recorded in the persistence layer
    */
  @WorkflowMethod def runSingleNodeStep(nodeExecutionRequest: NodeExecutionRequest): Unit;
}

/** Note: Constructor-based dependency injection is not supported in Temporal workflows.
  * See: https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeSingleStepWorkflowImpl extends NodeSingleStepWorkflow {

  // Default partition spec used for tests
//  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    WorkflowOperations.activityOptions
  )

  private def getCurrentTimeString: String = {
    // Get current time as milliseconds
    val currentTimeMillis = Workflow.currentTimeMillis

    // Convert milliseconds to Instant string
    java.time.Instant.ofEpochMilli(currentTimeMillis).toString
  }

  override def runSingleNodeStep(nodeExecutionRequest: NodeExecutionRequest): Unit = {
    // Get the workflow run ID
    val workflowRunId = Workflow.getInfo.getRunId

    // Create a NodeRun object with "WAITING" status
    val nodeRun = NodeRun(
      nodeName = nodeExecutionRequest.nodeName,
      startPartition = nodeExecutionRequest.partitionRange.start,
      endPartition = nodeExecutionRequest.partitionRange.end,
      runId = workflowRunId,
      branch = nodeExecutionRequest.branch,
      startTime = getCurrentTimeString,
      endTime = None,
      status = NodeRunStatus.WAITING
    )

    // Register the node run to persist the state
    activity.registerNodeRun(nodeRun)

    // Fetch dependencies after registering the node run
    val dependencies = activity.getTableDependencies(nodeExecutionRequest.nodeName)

    // Start multiple activities asynchronously
    val promises =
      for (dependency <- dependencies)
        yield {
          val dependencyPartitionRange =
            computeInputRange(nodeExecutionRequest.partitionRange, dependency.tableDependency)
          Async.function(
            activity.triggerDependency,
            NodeExecutionRequest(dependency.childNodeName, nodeExecutionRequest.branch, dependencyPartitionRange.get))
        }

    // Wait for all dependencies to complete
    Promise.allOf(promises.toSeq: _*).get()

    // Submit job after all dependencies are met
    activity.submitJob(nodeExecutionRequest.nodeName)

    // Update the node run status to "SUCCESS" after successful job submission
    // TODO: Ideally Agent need to update the status of node run and we should be waiting for it to succeed or fail here
    val completedNodeRun = nodeRun.copy(
      endTime = Some(getCurrentTimeString),
      status = NodeRunStatus.SUCCEEDED
    )
    activity.updateNodeRunStatus(completedNodeRun)
  }
}
