package ai.chronon.orchestration.temporal.constants

sealed trait TaskQueue extends Serializable

case object DAGExecutionWorkflowTaskQueue extends TaskQueue
