package ai.chronon.orchestration.temporal.workflow

import ai.chronon.api.ScalaJavaConversions.ListOps
import io.temporal.workflow.{QueryMethod, SignalMethod, Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.{DummyNodeGraph, DagExecutionWorkflowState}

import java.util

/** TODO: Add more documentation
  * Gets the workflow DAG from the CLI and executes it
  */
@WorkflowInterface
trait DagExecutionWorkflow {
  @WorkflowMethod def run(prevDagExecutionWorkflowState: DagExecutionWorkflowState): Unit;

  @SignalMethod def executeDag(dag: DummyNodeGraph): Unit

  @QueryMethod def getProcessedDagCount: Long
}

class DagExecutionWorkflowImpl extends DagExecutionWorkflow {
  private var dagExecutionWorkflowState = new DagExecutionWorkflowState()
    .setProcessedDagCount(0L)
    .setMaxHistoryLength(25000)
    .setPendingDags(new util.ArrayList[DummyNodeGraph]())

  override def run(prevDagExecutionWorkflowState: DagExecutionWorkflowState): Unit = {
    // Initialize state from previous execution if any
    dagExecutionWorkflowState = prevDagExecutionWorkflowState
    val dagQueue = dagExecutionWorkflowState.pendingDags

    while (true) {
      Workflow.await(() => !dagQueue.isEmpty)

      while (!dagQueue.isEmpty) {
        val dag = dagQueue.remove(0)
        processDag(dag)
        dagExecutionWorkflowState.processedDagCount += 1

        if (Workflow.getInfo.getHistoryLength > dagExecutionWorkflowState.maxHistoryLength) {
          Workflow.continueAsNew(dagExecutionWorkflowState)
          return
        }
      }
    }
  }

  override def executeDag(dag: DummyNodeGraph): Unit = {
    dagExecutionWorkflowState.addToPendingDags(dag)
  }

  override def getProcessedDagCount: Long = dagExecutionWorkflowState.processedDagCount

  private def processDag(dag: DummyNodeGraph): Unit = {
    if (dag.leafNodes == null) return
    for (leafNode <- dag.leafNodes.toScala) {
      // TODO: Recursively trigger node activities by traversing bottom up
    }
  }
}
