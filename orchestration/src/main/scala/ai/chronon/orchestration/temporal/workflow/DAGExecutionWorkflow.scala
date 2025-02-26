package ai.chronon.orchestration.temporal.workflow

import ai.chronon.api.ScalaJavaConversions.ListOps
import io.temporal.workflow.{QueryMethod, SignalMethod, Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.PhysicalNodeGraph

import scala.collection.mutable

/** TODO: Add more documentation
  * Gets the workflow DAG from the CLI and executes it
  */
@WorkflowInterface
trait DAGExecutionWorkflow {
  @WorkflowMethod def run(maxHistoryLength: Int): Unit;

  @SignalMethod def executeDag(dag: PhysicalNodeGraph): Unit;

  @QueryMethod def getStatus: String

  @QueryMethod def getProcessedDagCount: Int

  @QueryMethod def getHistoryLength: Long
}

class DAGExecutionWorkflowImpl extends DAGExecutionWorkflow {
  private var status: String = "IDLE"
  private var processedDagCount: Int = 0
  private var currentHistoryLength: Long = 0
  // Use a queue to store incoming DAGs
  private val dagQueue = new mutable.Queue[PhysicalNodeGraph]()

  override def run(maxHistoryLength: Int): Unit = {
    while (true) {
      Workflow.await(() => dagQueue.nonEmpty)

      while (dagQueue.nonEmpty) {
        status = "PROCESSING"
        val dag = dagQueue.dequeue()
        processDag(dag)
        processedDagCount += 1

        // Update history length
        currentHistoryLength = Workflow.getInfo.getHistoryLength

        if (Workflow.getInfo.getHistoryLength > maxHistoryLength) {
          status = "CONTINUING_AS_NEW"
          Workflow.continueAsNew(maxHistoryLength.asInstanceOf[Object])
          return
        }
      }

      status = "IDLE"
    }
  }

  override def executeDag(dag: PhysicalNodeGraph): Unit = {
    dagQueue.enqueue(dag)
  }

  override def getStatus: String = status

  override def getProcessedDagCount: Int = processedDagCount

  override def getHistoryLength: Long = currentHistoryLength

  private def processDag(dag: PhysicalNodeGraph): Unit = {
    if (dag.leafNodes == null) return
    for (leafNode <- dag.leafNodes.toScala) {
      // TODO: Recursively trigger node activities by traversing bottom up
    }
  }
}
