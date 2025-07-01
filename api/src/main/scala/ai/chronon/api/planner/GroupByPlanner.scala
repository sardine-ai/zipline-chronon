package ai.chronon.api.planner

import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, WindowUtils}
import ai.chronon.planner.{ConfPlan, GroupByBackfillNode, GroupByUploadNode, GroupByUploadToKVNode, Node}
import scala.collection.JavaConverters._

case class GroupByPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[GroupBy](groupBy)(outputPartitionSpec) {

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private val groupByTableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  def backfillNode: Node = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val effectiveStepDays =
      Option(groupBy.metaData.executionInfo).filter(_.isSetStepDays).map(_.stepDays).getOrElse(defaultStepDays)

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "backfill",
                                       groupBy.metaData.name + "/backfill",
                                       groupByTableDeps,
                                       Option(effectiveStepDays))

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toNode(metaData, _.setGroupByBackfill(node), semanticGroupBy(groupBy))
  }

  private def semanticGroupBy(groupBy: GroupBy): GroupBy = {
    val semanticGroupBy = groupBy.deepCopy()
    semanticGroupBy.unsetMetaData()
    semanticGroupBy
  }

  def uploadNode: Node = {
    val stepDays = 1 // GBUs write out data per day
    val metaData =
      MetaDataUtils.layer(groupBy.metaData,
                          "upload",
                          groupBy.metaData.name + "/upload",
                          groupByTableDeps,
                          Some(stepDays))

    val node = new GroupByUploadNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUpload(node), semanticGroupBy(groupBy))
  }

  def uploadToKVNode: Node = {
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(groupBy.metaData.uploadTable)
          .setPartitionColumn(outputPartitionSpec.column)
          .setPartitionFormat(outputPartitionSpec.format)
          .setPartitionInterval(WindowUtils.hours(outputPartitionSpec.spanMillis))
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val uploadToKVTableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(groupBy.metaData,
                          "uploadToKV",
                          groupBy.metaData.name + "/uploadToKV",
                          uploadToKVTableDeps,
                          None)

    val node = new GroupByUploadToKVNode().setGroupBy(eraseExecutionInfo)
    toNode(metaData, _.setGroupByUploadToKV(node), semanticGroupBy(groupBy))
  }

  override def buildPlan: ConfPlan = {
    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> backfillNode.metaData.name,
      ai.chronon.planner.Mode.DEPLOY -> uploadToKVNode.metaData.name
    )
    val confPlan = new ConfPlan()
      .setNodes(List(backfillNode, uploadNode, uploadToKVNode).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan
  }
}
