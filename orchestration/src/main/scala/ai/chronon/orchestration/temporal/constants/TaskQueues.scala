package ai.chronon.orchestration.temporal.constants

/** Defines all task queues enums used across workflows
  */
sealed trait TaskQueue extends Serializable

case object DagExecutionWorkflowTaskQueue extends TaskQueue

case object NodeExecutionWorkflowTaskQueue extends TaskQueue
