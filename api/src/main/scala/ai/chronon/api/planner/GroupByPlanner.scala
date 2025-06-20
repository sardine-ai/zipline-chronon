package ai.chronon.api.planner
import ai.chronon.api.{DataModel, GroupBy, PartitionSpec, TableDependency}
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.planner.{ConfPlan, GroupByBackfillNode, Node}
import scala.collection.JavaConverters._

class GroupByPlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends Planner[GroupBy](groupBy)(outputPartitionSpec) {

  private def tableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  private def effectiveStepDays: Int = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val configuredStepDaysOpt = Option(groupBy.metaData.executionInfo).flatMap(e => Option(e.stepDays))
    configuredStepDaysOpt.getOrElse(defaultStepDays)
  }

  // execInfo can be heavy - and we don't want to duplicate it
  private def eraseExecutionInfo: GroupBy = {
    val result = groupBy.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  def backfillNode: Node = {

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "backfill",
                                       groupBy.metaData.name + "/backfill",
                                       tableDeps,
                                       Some(effectiveStepDays))

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toNode(metaData, _.setGroupByBackfill(node), eraseExecutionInfo)
  }

  def uploadNode: Node = {

    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "upload",
                                       groupBy.metaData.name + "/upload",
                                       tableDeps,
                                       Some(effectiveStepDays))

    metaData.executionInfo.unsetOutputTableInfo()

    val node = new GroupByBackfillNode().setGroupBy(eraseExecutionInfo)

    toNode(metaData, _.setGroupByBackfill(node), eraseExecutionInfo)
  }

  override def buildPlan: ConfPlan = {
    val bFillNode = backfillNode
    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> bFillNode.metaData.name
    )
    val confPlan = new ConfPlan()
      .setNodes(List(bFillNode).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
    confPlan.setNodes(List(bFillNode).asJava)
    confPlan
  }
}
