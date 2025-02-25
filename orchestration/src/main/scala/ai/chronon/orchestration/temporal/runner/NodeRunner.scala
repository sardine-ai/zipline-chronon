package ai.chronon.orchestration.temporal.runner

import ai.chronon.orchestration.PhysicalNodeInstance

class NodeRunner(node: PhysicalNodeInstance) {

  // main run method for the runner
  // triggers the right backfill or frontfill or other methods depending on the physical node type
  def run(): Unit = ???

  def backfill(): Unit = ???

  def frontfill(): Unit = ???

  // returns list of dependency nodes for a given physical node
  def getDependencies(): Seq[PhysicalNodeInstance] = ???

  // trigger new workflow or wait for an already running workflow for each dependency node
  def triggerAndWaitForDependencies(): Unit = ???

  // submit the job for the node to the agent when the dependencies are met
  def submitJob(): Unit = ???
}
