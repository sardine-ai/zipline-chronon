package ai.chronon.orchestration.physical

import ai.chronon.online.Api
import ai.chronon.orchestration._

// all nodes that purely take tables and produce tables
abstract class TabularNode[T](val conf: T) extends ComputeNodeRunner[T](conf) {
  def tableDependencies: Seq[TableDependency]
  def outputTable: String
  def nodeType: PhysicalNodeType

  // write data or partition status etc into the api while/after doing the task
  // the orchestrator will wrap this in a workflow with partition sensors and retries
  // TODO: implement in a later PR
  def run(api: Api): Unit = ???

  override def dependencies: Seq[Dependency] =
    tableDependencies.map { td =>
      val result = new Dependency()
      result.setTableDependency(td)
      result
    }

  override def output: Artifact = {
    val result = new Artifact()
    result.setTable(new Table().setTable(outputTable))
    result
  }
}
