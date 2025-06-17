package ai.chronon.api.planner

import ai.chronon.api.{StagingQuery, PartitionSpec}
import ai.chronon.planner.ConfPlan
import scala.collection.JavaConverters._

case class StagingQueryPlanner(stagingQuery: StagingQuery)(implicit outputPartitionSpec: PartitionSpec)
    extends Planner[StagingQuery](stagingQuery)(outputPartitionSpec) {

  override def buildPlan: ConfPlan = {

    val metaData = MetaDataUtils.layer(
      stagingQuery.metaData,
      "backfill",
      stagingQuery.metaData.name + "/backfill",
      TableDependencies.fromStagingQuery(stagingQuery),
      Some(1) // Default step days for staging queries
    )

    val node = new ai.chronon.planner.StagingQueryNode().setStagingQuery(stagingQuery)

    val finalNode = toNode(metaData, _.setStagingQuery(node), stagingQuery)

    val terminalNodeNames = Map(
      ai.chronon.planner.Mode.BACKFILL -> finalNode.metaData.name
    )

    new ConfPlan()
      .setNodes(List(finalNode).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}
