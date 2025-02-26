package ai.chronon.orchestration.temporal.runner

import ai.chronon.orchestration.PhysicalNodeInstance

class PartitionRunner(node: PhysicalNodeInstance) extends NodeRunner(node) {

  def backfill(): Unit = ???

  def frontfill(): Unit = ???
}
