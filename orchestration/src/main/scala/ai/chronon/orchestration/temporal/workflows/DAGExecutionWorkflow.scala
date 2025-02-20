package ai.chronon.orchestration.temporal.workflows

import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import ai.chronon.orchestration.PhysicalNodeGraph

@WorkflowInterface
trait DAGExecutionWorkflow {
  @WorkflowMethod def execute(dag: PhysicalNodeGraph): Unit
}

class DAGExecutionWorkflowImpl extends DAGExecutionWorkflow {

  override def execute(dag: PhysicalNodeGraph): Unit = {
    print("Workflow successfully executed")
  }
}

