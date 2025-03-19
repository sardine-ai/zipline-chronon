package ai.chronon.orchestration.temporal.workflow

import ai.chronon.api.ScalaJavaConversions.ListOps
import io.temporal.workflow.{Async, Promise, Workflow, WorkflowInterface, WorkflowMethod}
import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivity
import io.temporal.activity.ActivityOptions

import java.time.Duration
import java.util

/** Workflow for individual node execution with in a DAG
  *
  * At a high level we need to do the following steps
  * 1. Find missing partitions and compute step ranges
  * 2. For each missing step range trigger all the dependency node workflows and wait for them to complete
  * 3. Submit the job to the agent when all the dependencies are met
  */
@WorkflowInterface
trait NodeExecutionWorkflow {
  @WorkflowMethod def executeNode(node: DummyNode): Unit;
}

/** Dependency injection through constructor for workflows is not directly supported
  * https://community.temporal.io/t/complex-workflow-dependencies/511
  */
class NodeExecutionWorkflowImpl extends NodeExecutionWorkflow {

  // TODO: To make the activity options configurable
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofMinutes(10))
      .build()
  )

  override def executeNode(node: DummyNode): Unit = {
    // TODO: To trigger new activity task for finding missing partitions and compute missing steps
    val dependencies = getDependencies(node)

    // TODO: To trigger dependency runs for all missing partitions
    // Start multiple activities asynchronously
    val promises =
      for (dep <- dependencies.toScala)
        yield {
          Async.function(activity.triggerDependency, dep)
        }

    // Wait for all dependencies to complete
    Promise.allOf(promises.toSeq: _*).get()

    // Submit job after all dependencies are met
    activity.submitJob(node)
  }

  private def getDependencies(node: DummyNode): util.List[DummyNode] = {
    if (node.getDependencies == null) {
      new util.ArrayList[DummyNode]()
    } else {
      node.getDependencies
    }
  }
}
