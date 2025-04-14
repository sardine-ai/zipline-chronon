package ai.chronon.orchestration.utils

import ai.chronon.orchestration.temporal.NodeExecutionRequest

object TemporalUtils {

  def getNodeSingleStepWorkflowId(nodeExecutionRequest: NodeExecutionRequest): String = {
    val name = nodeExecutionRequest.nodeName.name
    val start = nodeExecutionRequest.partitionRange.start
    val end = nodeExecutionRequest.partitionRange.end
    s"single-step/$name[$start to $end]"
  }

  def getNodeRangeCoordinatorWorkflowId(nodeExecutionRequest: NodeExecutionRequest): String = {
    val name = nodeExecutionRequest.nodeName.name
    val start = nodeExecutionRequest.partitionRange.start
    val end = nodeExecutionRequest.partitionRange.end
    s"range-coordinator/$name[$start to $end]"
  }

}
