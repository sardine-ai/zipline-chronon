package ai.chronon.orchestration.temporal.constants

/** Task queues for Temporal workflow and activity routing.
  *
  * A TaskQueue is a lightweight, dynamically allocated queue that one or more workers poll for tasks.
  * In Temporal, task queues:
  * - Route workflow and activity tasks to appropriate workers
  * - Enable horizontal scaling by distributing load across multiple workers
  * - Allow for specialized workers that handle specific workflows or activities
  * - Support independent scaling of different workflow/activity types
  *
  * Using separate task queues for different workflow types allows for:
  * - Separate rate limiting and resource allocation
  * - Independent scaling based on workload characteristics
  * - Logical separation of processing concerns
  *
  * For more details, see: [Temporal Task Queues](https://docs.temporal.io/task-queue)
  */
sealed trait TaskQueue extends Serializable

case object NodeSingleStepWorkflowTaskQueue extends TaskQueue

case object NodeRangeCoordinatorWorkflowTaskQueue extends TaskQueue
