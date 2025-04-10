package ai.chronon.api.planner

abstract class Planner[T](conf: T)(implicit outputPartitionSpec: PartitionSpecWithColumn) {
  def offlineNodes: Seq[PlanNode]
  def onlineNodes: Seq[PlanNode]
  def metricsNodes: Seq[PlanNode] = ??? // TODO: Add later
}
