package ai.chronon.orchestration.temporal.activity

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.storage.DependencyStorage
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable

@ActivityInterface trait NodeRunnerActivity {

  /** Does one of the following steps in order for each node dependency
    *  1. Progress further for already completed node dependency workflow by reading from storage
    *  2. Wait for currently running node dependency workflow if it's already triggered
    *  3. Trigger a new node dependency workflow run
    */
  @ActivityMethod def triggerAndWaitForDependencies(node: DummyNode): Unit

  // Submits the job for the node to the agent when the dependencies are met
  @ActivityMethod def submitJob(node: DummyNode): Unit
}

class NodeRunnerActivityImpl(
    workflowOps: WorkflowOperations,
    storage: DependencyStorage
) extends NodeRunnerActivity {

  override def triggerAndWaitForDependencies(node: DummyNode): Unit = {
    if (node.getDependencies == null || node.getDependencies.isEmpty) return

    val dependencies = getDependencies(node)
    val futures = new mutable.ListBuffer[CompletableFuture[Void]]()

    for (dep <- dependencies.toScala) {
      // TODO: To properly cover all three cases as mentioned in the above interface definition
      if (!storage.isDependencyCompleted(dep.getName)) {
        futures += workflowOps.startNodeWorkflow(dep)
      }
    }

    if (futures.nonEmpty) {
      CompletableFuture.allOf(futures: _*).get()
    }
  }

  override def submitJob(node: DummyNode): Unit = {
    // TODO: Implementation for job submission
  }

  private def getDependencies(node: DummyNode): util.List[DummyNode] = {

    /** TODO tasks
      *  1. To split to multiple dependency nodes based on partition ranges, step size and offset
      *  2. Read from storage to figure out if the child dependency nodes are already filled
      */
    node.getDependencies
  }
}
