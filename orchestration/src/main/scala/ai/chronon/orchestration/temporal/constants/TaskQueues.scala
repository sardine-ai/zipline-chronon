package ai.chronon.orchestration.temporal.constants

/** A TaskQueue is a light weight dynamically allocated queue that one or more workers poll for tasks
  * We can have separate task queues for workflows and activities for clear separation if needed, also for
  * proper load balancing and can scale independently. more details [here](https://docs.temporal.io/task-queue)
  *
  * Defines task queue enums for all workflows and activities
  */
sealed trait TaskQueue extends Serializable

// TODO: To look into if we really need to have separate task queues for node execution workflow and activity
case object NodeExecutionWorkflowTaskQueue extends TaskQueue
