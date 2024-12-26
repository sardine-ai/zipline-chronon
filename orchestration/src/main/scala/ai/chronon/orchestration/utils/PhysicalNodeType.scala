package ai.chronon.orchestration.utils

import ai.chronon.orchestration.GroupByNodeType
import ai.chronon.orchestration.JoinNodeType
import ai.chronon.orchestration.ModelNodeType
import ai.chronon.orchestration.PhysicalNodeType
import ai.chronon.orchestration.StagingQueryNodeType

object PhysicalNodeType {

  def from(groupByNodeType: GroupByNodeType): PhysicalNodeType = {
    val result = new PhysicalNodeType()
    result.setGroupByNodeType(groupByNodeType)
    result
  }

  def from(joinNodeType: JoinNodeType): PhysicalNodeType = {
    val result = new PhysicalNodeType()
    result.setJoinNodeType(joinNodeType)
    result
  }

  def from(stagingQueryNodeType: StagingQueryNodeType): PhysicalNodeType = {
    val result = new PhysicalNodeType()
    result.setStagingNodeType(stagingQueryNodeType)
    result
  }

  def from(modelNodeType: ModelNodeType): PhysicalNodeType = {
    val result = new PhysicalNodeType()
    result.setModelNodeType(modelNodeType)
    result
  }

}
