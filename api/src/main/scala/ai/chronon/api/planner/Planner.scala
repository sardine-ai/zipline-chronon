package ai.chronon.api.planner

import ai.chronon.api.PartitionSpec

import scala.collection.Seq

abstract class Planner[T](conf: T)(implicit outputPartitionSpec: PartitionSpec) {
  def offlineNodes: Seq[PlanNode]
  def onlineNodes: Seq[PlanNode]
  def metricsNodes: Seq[PlanNode] = ??? // TODO: Add later
}
