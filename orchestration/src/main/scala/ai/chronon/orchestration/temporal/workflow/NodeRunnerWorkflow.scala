package ai.chronon.orchestration.temporal.workflow

import ai.chronon.orchestration.PhysicalNodeInstance
import ai.chronon.orchestration.temporal.runner.NodeRunner
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/** Workflow wrapper around node runner
  */
@WorkflowInterface
trait NodeRunnerWorkflow {
  @WorkflowMethod def run(): Unit
}

class NodeRunnerWorkflowImpl(nodeRunner: NodeRunner) extends NodeRunnerWorkflow {
  override def run(): Unit = {
    nodeRunner.run()
    print("Node runner workflow successfully completed")
  }
}
