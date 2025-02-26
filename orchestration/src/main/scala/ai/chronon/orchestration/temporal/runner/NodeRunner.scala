package ai.chronon.orchestration.temporal.runner

import ai.chronon.orchestration.PhysicalNodeInstance

/** TODO: This can also be implemented as an activity with inheritance
  * @param node - The physical node instance to execute with in the DAG
  */
class NodeRunner(node: PhysicalNodeInstance) {

  /** Main run method for the runner triggers the right backfill or frontfill or other methods
    * depending on the physical node type
    */
  def run(): Unit = ???

  // returns list of dependency nodes for a given physical node
  def getDependencies(): Seq[PhysicalNodeInstance] = ???

  /** Does one of the following steps in order for each dependency
    *  1. Progress further for already completed activities by reading from storage
    *  2. Wait for currently running activity if it's already triggered
    *  3. Trigger a new activity run
    */
  def triggerAndWaitForDependencies(): Unit = ???

  // submit the job for the node to the agent when the dependencies are met
  def submitJob(): Unit = ???
}
