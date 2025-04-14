package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.persistence.{NodeDao, NodeRun, NodeTableDependency}
import ai.chronon.orchestration.pubsub.{JobSubmissionMessage, PubSubPublisher}
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import ai.chronon.orchestration.utils.TemporalUtils
import ai.chronon.api.PartitionRange
import ai.chronon.api.planner.DependencyResolver
import ai.chronon.orchestration.NodeRunStatus
import io.temporal.activity.{Activity, ActivityInterface, ActivityMethod}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import java.util.concurrent.CompletableFuture

/** Temporal Activity interface for node execution management.
  *
  * This interface defines activities used by Temporal workflows to execute and manage node processing.
  * These activities handle critical operations such as:
  * - Triggering node dependencies in the correct order
  * - Managing node execution state and history
  * - Submitting jobs to execution agents
  * - Tracking node relationships and dependencies
  *
  * Activities abstract complex operations that may interact with external systems like databases,
  * message queues, and other services, enabling idempotent, reliable execution through Temporal.
  */
@ActivityInterface trait NodeExecutionActivity {

  /** Triggers a dependency node execution workflow.
    *
    * @param nodeExecutionRequest The execution parameters including node, branch, and time range
    */
  @ActivityMethod def triggerDependency(nodeExecutionRequest: NodeExecutionRequest): Unit

  /** Submits a job for execution to the compute agent.
    *
    * This method publishes a job message to the message queue for a compute agent to pick up
    * and execute when dependencies are met.
    *
    * @param nodeName The node to execute
    */
  @ActivityMethod def submitJob(nodeName: NodeName): Unit

  /** Retrieves the downstream table dependencies for a given node.
    *
    * @param nodeName The node to find dependencies for
    * @return A sequence of node table dependencies that depend on the specified node
    */
  @ActivityMethod def getTableDependencies(nodeName: NodeName): Seq[NodeTableDependency]

  /** Identifies missing partition ranges that need to be processed.
    *
    * @param nodeExecutionRequest The execution request containing node, branch, and time range
    * @return Sequence of partition ranges that need to be processed
    */
  @ActivityMethod def getMissingSteps(nodeExecutionRequest: NodeExecutionRequest): Seq[PartitionRange]

  /** Triggers workflows for missing partition steps.
    *
    * This activity analyzes the missing steps and does one of:
    * - Skips steps that already completed successfully
    * - Retries steps that previously failed
    * - Waits for steps that are currently running
    * - Starts new workflows for steps that haven't been processed
    *
    * @param nodeName The node to process
    * @param branch The branch context
    * @param missingSteps The sequence of partition ranges to process
    */
  @ActivityMethod def triggerMissingNodeSteps(nodeName: NodeName,
                                              branch: Branch,
                                              missingSteps: Seq[PartitionRange]): Unit

  /** Registers a new node run in the persistence layer.
    *
    * @param nodeRun The node run entry to register
    */
  @ActivityMethod def registerNodeRun(nodeRun: NodeRun): Unit

  /** Updates the status of an existing node run.
    *
    * @param updatedNodeRun The node run with updated status
    */
  @ActivityMethod def updateNodeRunStatus(updatedNodeRun: NodeRun): Unit
}

/** Dependency injection through constructor is supported for activities but not for workflows.
  * See: https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeExecutionActivityImpl(
    workflowOps: WorkflowOperations,
    nodeDao: NodeDao,
    pubSubPublisher: PubSubPublisher
) extends NodeExecutionActivity {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Helper method to handle asynchronous completion of Temporal activities.
    *
    * This approach is necessary for activities that involve asynchronous operations
    * like workflow invocations or message publishing. It ensures that the activity
    * only completes when the underlying async operation is actually done, not just
    * when it's been initiated.
    */
  private def handleAsyncCompletion[T](future: CompletableFuture[T]): Unit = {
    val context = Activity.getExecutionContext
    context.doNotCompleteOnReturn()

    val completionClient = context.useLocalManualCompletion()

    future.whenComplete((result, error) => {
      if (error != null) {
        logger.error(s"Activity failed with following error: ", error)
        completionClient.fail(error)
      } else {
        completionClient.complete(result)
      }
    })
  }

  override def triggerDependency(nodeExecutionRequest: NodeExecutionRequest): Unit = {
    // Start the workflow
    val future = workflowOps.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest)

    // Handle async completion
    handleAsyncCompletion(future)
  }

  override def submitJob(nodeName: NodeName): Unit = {
    logger.info(s"Submitting job for node: $nodeName")

    // Create a message from the node
    val message = JobSubmissionMessage.fromNodeName(nodeName)

    // Publish the message
    val future = pubSubPublisher.publish(message)

    // Handle async completion
    handleAsyncCompletion(future)
  }

  override def getTableDependencies(nodeName: NodeName): Seq[NodeTableDependency] = {
    try {
      val result = Await.result(nodeDao.getNodeTableDependencies(nodeName), 1.seconds)
      logger.info(s"Successfully pulled the dependencies for node: $nodeName")
      result
    } catch {
      case e: Exception =>
        val errorMsg = s"Error pulling dependencies for node: $nodeName"
        logger.error(errorMsg)
        throw new RuntimeException(errorMsg, e)
    }
  }

  /** Identifies successfully completed partitions from previous node runs. */
  private def getExistingPartitions(nodeExecutionRequest: NodeExecutionRequest): Seq[String] = {
    // Find all node runs that overlap with the requested partition range
    val nodeRuns = findOverlappingNodeRuns(nodeExecutionRequest)

    // Expand each node run into individual partitions with metadata
    val partitionsWithMetadata = nodeRuns.flatMap { nodeRun =>
      // Convert node run's start/end into a partition range
      val partitionRange = PartitionRange(
        nodeRun.startPartition,
        nodeRun.endPartition
      )(nodeExecutionRequest.partitionRange.partitionSpec)

      // Create tuples of (partition, startTime, endTime, status) for each partition in the range
      partitionRange.partitions.map { partition =>
        (partition, nodeRun.startTime, nodeRun.endTime, nodeRun.status)
      }
    }

    // Process partitions to find successfully completed ones
    partitionsWithMetadata
      .groupBy(_._1) // Group by partition
      .map { case (_, tuples) => // For each partition group
        // First check if there are any runs without end time (currently running)
        val ongoingRuns = tuples.filter(_._3.isEmpty)
        if (ongoingRuns.nonEmpty) {
          // If there are ongoing runs, pick the one with latest start time
          ongoingRuns.maxBy(_._2)
        } else {
          // Otherwise, pick the one with latest end time
          tuples.maxBy(_._3)
        }
      }
      .filter(_._4 == NodeRunStatus.SUCCEEDED) // Keep only completed partitions
      .map(_._1) // Extract just the partition string
      .toSeq
  }

  private def getStepDays(nodeName: NodeName): StepDays = {
    try {
      val result = Await.result(nodeDao.getStepDays(nodeName), 1.seconds)
      logger.info(s"Found step days for ${nodeName.name}: ${result.stepDays}")
      result
    } catch {
      case e: Exception =>
        val errorMsg = s"Error finding step days for ${nodeName.name}"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }

  override def getMissingSteps(nodeExecutionRequest: NodeExecutionRequest): Seq[PartitionRange] = {
    DependencyResolver.getMissingSteps(
      nodeExecutionRequest.partitionRange,
      getExistingPartitions(nodeExecutionRequest),
      getStepDays(nodeExecutionRequest.nodeName).stepDays
    )
  }

  override def triggerMissingNodeSteps(nodeName: NodeName, branch: Branch, missingSteps: Seq[PartitionRange]): Unit = {
    // Trigger missing node steps
    val futures = missingSteps.map { missingStep =>
      val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, missingStep)

      // Check if a node run already exists for this step
      val existingRun = findLatestCoveringRun(nodeExecutionRequest)

      existingRun match {
        case Some(nodeRun) =>
          // A run exists, decide what to do based on its status
          nodeRun.status match {
            case NodeRunStatus.SUCCEEDED =>
              // Already completed successfully, nothing to do
              logger.info(
                s"NodeRun for $nodeName on $branch from ${missingStep.start} to ${missingStep.end} already succeeded, skipping")
              CompletableFuture.completedFuture[Void](null)

            case NodeRunStatus.WAITING | NodeRunStatus.RUNNING =>
              // Run is already in progress, wait for it
              logger.info(
                s"NodeRun for $nodeName on $branch from ${missingStep.start} to ${missingStep.end} is already in progress (${nodeRun.status}), waiting")
              val workflowId = TemporalUtils.getNodeSingleStepWorkflowId(nodeExecutionRequest)
              workflowOps.getWorkflowResult(workflowId, nodeRun.runId)

            case _ =>
              // failed or other unknown status, try again
              logger.info(
                s"NodeRun for $nodeName on $branch from ${missingStep.start} to ${missingStep.end} has failed/unknown status ${nodeRun.status}, retrying")
              workflowOps.startNodeSingleStepWorkflow(nodeExecutionRequest)
          }

        case None =>
          // No existing run, start a new workflow
          logger.info(
            s"No existing NodeRun for $nodeName on $branch from ${missingStep.start} to ${missingStep.end}, starting new workflow")
          workflowOps.startNodeSingleStepWorkflow(nodeExecutionRequest)
      }
    }

    // Handle async completion
    handleAsyncCompletion(CompletableFuture.allOf(futures.toSeq: _*))
  }

  override def registerNodeRun(nodeRun: NodeRun): Unit = {
    try {
      Await.result(nodeDao.insertNodeRun(nodeRun), 1.seconds)
      logger.info(s"Successfully registered the node run: ${nodeRun}")
    } catch {
      case e: Exception =>
        if (e.getMessage != null && e.getMessage.contains("ALREADY_EXISTS")) {
          logger.info(s"Already registered $nodeRun, skipping creation")
        } else {
          logger.error(s"Error registering the node run: $nodeRun")
          throw e
        }
    }
  }

  override def updateNodeRunStatus(updatedNodeRun: NodeRun): Unit = {
    try {
      Await.result(nodeDao.updateNodeRunStatus(updatedNodeRun), 1.seconds)
      logger.info(s"Successfully updated the status of run ${updatedNodeRun.runId} to ${updatedNodeRun.status}")
    } catch {
      case e: Exception =>
        val errorMsg = s"Error updating status of run ${updatedNodeRun.runId} to ${updatedNodeRun.status}"
        logger.error(errorMsg)
        throw new RuntimeException(errorMsg, e)
    }
  }

  // Find all node runs overlapping with partitionRange from nodeExecutionRequest and is relevant for the
  // context of missing node ranges
  private def findOverlappingNodeRuns(nodeExecutionRequest: NodeExecutionRequest): Seq[NodeRun] = {
    try {
      val result = Await.result(nodeDao.findOverlappingNodeRuns(nodeExecutionRequest), 1.seconds)
      logger.info(s"Found overlapping node runs for $nodeExecutionRequest: $result")
      result
    } catch {
      case e: Exception =>
        val errorMsg = s"Error finding overlapping node runs for $nodeExecutionRequest"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }

  private def findLatestCoveringRun(nodeExecutionRequest: NodeExecutionRequest): Option[NodeRun] = {
    try {
      val result = Await.result(nodeDao.findLatestCoveringRun(nodeExecutionRequest), 1.seconds)
      logger.info(s"Found latest node run for $nodeExecutionRequest: $result")
      result
    } catch {
      case e: Exception =>
        val errorMsg = s"Error finding latest node run for $nodeExecutionRequest"
        logger.error(errorMsg, e)
        throw new RuntimeException(errorMsg, e)
    }
  }
}
