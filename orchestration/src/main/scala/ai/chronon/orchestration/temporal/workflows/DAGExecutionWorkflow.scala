package ai.chronon.orchestration.temporal.workflows

import ai.chronon.api.ScalaJavaConversions.ListOps
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import ai.chronon.orchestration.PhysicalNodeGraph

/** Gets the workflow DAG from the CLI and executes it
  */
@WorkflowInterface
trait DAGExecutionWorkflow {
  @WorkflowMethod def execute(dag: PhysicalNodeGraph): Unit
}

class DAGExecutionWorkflowImpl extends DAGExecutionWorkflow {

  override def execute(dag: PhysicalNodeGraph): Unit = {
//    for (leafNode <- dag.leafNodes.toScala) {
//      // Start NodeRunner workflows for each leaf node and wait for all of them to complete
//    }
    print("DAG execution workflow successfully completed")
  }
}
