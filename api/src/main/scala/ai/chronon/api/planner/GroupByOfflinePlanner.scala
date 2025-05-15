package ai.chronon.api.planner
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{DataModel, GroupBy, MetaData, PartitionSpec, TableDependency, ThriftJsonCodec}
import ai.chronon.orchestration.GroupByBackfillNode

import scala.util.Try

class GroupByOfflinePlanner(groupBy: GroupBy)(implicit outputPartitionSpec: PartitionSpec)
    extends Planner[GroupBy](groupBy)(outputPartitionSpec) {

  private def tableDeps: Seq[TableDependency] = TableDependencies.fromGroupBy(groupBy)

  private def effectiveStepDays: Int = {
    val defaultStepDays = if (groupBy.dataModel == DataModel.EVENTS) 15 else 1
    val configuredStepDaysOpt = Option(groupBy.metaData.executionInfo).flatMap(e => Option(e.stepDays))
    configuredStepDaysOpt.getOrElse(defaultStepDays)
  }

  val backfillNodeOpt: Option[GroupByBackfillNode] = for (execInfo <- Option(groupBy.metaData.executionInfo)) yield {
    val metaData = MetaDataUtils.layer(groupBy.metaData,
                                       "backfill",
                                       groupBy.metaData.name + "/backfill",
                                       tableDeps,
                                       Some(effectiveStepDays))
    metaData.executionInfo.setScheduleCron(execInfo.scheduleCron)
    new GroupByBackfillNode().setGroupBy(groupBy).setMetaData(metaData)
  }

  override def offlineNodes: Seq[PlanNode] = ???

  override def onlineNodes: Seq[PlanNode] = ???
}
object GroupByOfflinePlanner {
  implicit class GroupByIsPlanNode(node: GroupBy) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      result
    })
  }
}
