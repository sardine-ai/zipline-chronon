package ai.chronon.api.planner

import ai.chronon.api.{GroupBy, PartitionSpec, TableDependency}
import ai.chronon.planner.{ConfPlan, GroupByUploadNode, Node}

import scala.collection.JavaConverters._

case class GroupByUploadPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends Planner[GroupBy](groupBy)(outputPartitionSpec) {

  private def tableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)
  private val stepDays = 1 // GBUs write out data per day

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
    semanticGroupBy.unsetMetaData()
    semanticGroupBy
  }

  private def uploadNode: Node = {
    val metaData =
      MetaDataUtils.layer(groupBy.metaData, "upload", groupBy.metaData.name + "/upload", tableDeps, Some(stepDays))

    val node = new GroupByUploadNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUpload(node), semanticGroupBy(groupBy))
  }

  override def buildPlan: ConfPlan = {
    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.DEPLOY -> uploadNode.metaData.name
    )
    val confPlan = new ConfPlan()
      .setNodes(List(uploadNode).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan.setNodes(List(uploadNode).asJava)
    confPlan
  }
}
