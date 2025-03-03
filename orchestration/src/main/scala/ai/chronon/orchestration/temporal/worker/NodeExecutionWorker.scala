package ai.chronon.orchestration.temporal.worker

import ai.chronon.orchestration.temporal.activity.NodeRunnerActivityFactory
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.storage.DependencyStorage
import ai.chronon.orchestration.temporal.workflow.NodeExecutionWorkflowImpl
import io.temporal.client.{WorkflowClient, WorkflowClientOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.WorkerFactory

import scala.collection.mutable

object NodeExecutionWorker {

  // TODO: currently using dummy storage layer interface
  private val dependencyStorage = new DependencyStorage {
    private val completedDeps = mutable.Set[String]()

    override def isDependencyCompleted(nodeId: String): Boolean =
      completedDeps.contains(nodeId)

    override def markDependencyCompleted(nodeId: String): Unit =
      completedDeps.add(nodeId)
  }

  private def createWorkflowClient(): WorkflowClient = {
    // Create custom data converter with Thrift support
    val customDataConverter = DefaultDataConverter.newDefaultInstance
      .withPayloadConverterOverrides(new ThriftPayloadConverter)

    // Configure client options
    val clientOptions = WorkflowClientOptions
      .newBuilder()
      .setNamespace("default") // adjust namespace as needed
      .setIdentity("node-execution-runner")
      .setDataConverter(customDataConverter)
      .build()

    // Create service stubs and client
    val serviceStub = WorkflowServiceStubs.newLocalServiceStubs
    WorkflowClient.newInstance(serviceStub, clientOptions)
  }

  def main(args: Array[String]): Unit = {
    val client = createWorkflowClient()
    // A WorkerFactory creates Workers
    val factory = WorkerFactory.newInstance(client)
    // A Worker listens to one Task Queue.
    // This Worker processes both Workflows and Activities
    val worker = factory.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    // Register a Workflow implementation with this Worker
    // The implementation must be known at runtime to dispatch Workflow tasks
    // Workflows are stateful so a type is needed to create instances.
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])
    // Register Activity implementation(s) with this Worker.
    // The implementation must be known at runtime to dispatch Activity tasks
    // Activities are stateless and thread safe so a shared instance is used.
    worker.registerActivitiesImplementations(
      NodeRunnerActivityFactory.create(client, NodeExecutionWorkflowTaskQueue.toString, dependencyStorage))
    System.out.println("Worker is running and actively polling the Task Queue.")
    System.out.println("To quit, use ^C to interrupt.")
    // Start all registered Workers. The Workers will start polling the Task Queue.
    factory.start()
  }
}
