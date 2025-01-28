package ai.chronon.orchestration.physical

import ai.chronon.online.Api
import ai.chronon.orchestration.Artifact
import ai.chronon.orchestration.Dependency
import ai.chronon.orchestration.PhysicalNodeType

abstract class ComputeNodeRunner[T](conf: T) {

  def dependencies: Seq[Dependency]
  def output: Artifact
  def nodeType: PhysicalNodeType

  // write data or partition status etc into the api while/after doing the task
  // the orchestrator will wrap this in a workflow with partition sensors and retries
  // TODO: implement in a later PR
  def run(api: Api): Unit
}
