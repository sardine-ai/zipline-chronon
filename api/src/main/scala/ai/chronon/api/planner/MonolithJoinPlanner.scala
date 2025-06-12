package ai.chronon.api.planner

import ai.chronon.api.{Join, PartitionSpec}
import ai.chronon.planner

import scala.collection.JavaConverters._

class MonolithJoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends Planner[Join](join)(outputPartitionSpec) {

  private def effectiveStepDays: Int = {
    Option(join.metaData.executionInfo).map(_.stepDays).getOrElse(1)
  }

  override def buildPlan: planner.ConfPlan = {
    val confPlan = new planner.ConfPlan()

    val tableDeps = TableDependencies.fromJoin(join)

    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "backfill",
                          join.metaData.name + "/backfill",
                          tableDeps,
                          Some(effectiveStepDays))
    val node = new planner.MonolithJoinNode().setJoin(join)
    val finalNode = toNode(metaData, _.setMonolithJoin(node), join)

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> finalNode.metaData.name
    ).asJava
    confPlan.setNodes(List(finalNode).asJava).setTerminalNodeNames(terminalNodeNames)
  }
}
