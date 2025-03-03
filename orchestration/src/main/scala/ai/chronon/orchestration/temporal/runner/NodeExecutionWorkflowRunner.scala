package ai.chronon.orchestration.temporal.runner

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.workflow.NodeExecutionWorkflow
import ai.chronon.orchestration.utils.FuncUtils
import io.temporal.client.{WorkflowClient, WorkflowClientOptions, WorkflowOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.serviceclient.WorkflowServiceStubs

import java.util

object NodeExecutionWorkflowRunner {

  private val taskQueue = NodeExecutionWorkflowTaskQueue.toString

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

  def executeNode(client: WorkflowClient, node: DummyNode): Unit = {
    val workflowId = s"node-execution-${node.name}"

    val options = WorkflowOptions
      .newBuilder()
      .setTaskQueue(taskQueue)
      .setWorkflowId(workflowId)
      .build()

    val workflow = client.newWorkflowStub(classOf[NodeExecutionWorkflow], options)

    println(s"\nExecuting node: ${node.getName}")
    println(s"WorkflowId: $workflowId")

    try {
      // Start workflow asynchronously
      val execution = WorkflowClient.start(FuncUtils.toTemporalProc(workflow.executeNode(node)))
      println(s"Started workflow execution: ${execution.getRunId}")
    } catch {
      case e: Exception =>
        println(s"Error executing node: ${e.getMessage}")
    }
  }

  private def getSimpleNode: DummyNode = {
    val depNode1 = new DummyNode().setName("dep1")
    val depNode2 = new DummyNode().setName("dep2")
    new DummyNode()
      .setName("main")
      .setDependencies(util.Arrays.asList(depNode1, depNode2))
  }

  private def getComplexNode: DummyNode = {
    val stagingQuery1 = new DummyNode().setName("StagingQuery1")
    val stagingQuery2 = new DummyNode().setName("StagingQuery2")

    val groupBy1 = new DummyNode()
      .setName("GroupBy1")
      .setDependencies(util.Arrays.asList(stagingQuery1))

    val groupBy2 = new DummyNode()
      .setName("GroupBy2")
      .setDependencies(util.Arrays.asList(stagingQuery2))

    val join = new DummyNode()
      .setName("Join")
      .setDependencies(util.Arrays.asList(groupBy1, groupBy2))

    new DummyNode()
      .setName("Derivation")
      .setDependencies(util.Arrays.asList(join))
  }

  private def printMenu(): Unit = {
    println("\nSelect an option:")
    println("1. Execute Simple DAG (main -> dep1, dep2)")
    println("2. Execute Complex DAG (Derivation -> Join -> GroupBy -> StagingQuery)")
    println("3. Create Custom DAG")
    println("4. Exit")
    print("Enter your choice (1-4): ")
  }

  private def readNodeInput(parentNodeName: String = ""): DummyNode = {
    print(s"\nEnter node name${if (parentNodeName.nonEmpty) s" for $parentNodeName" else ""}: ")
    val nodeName = scala.io.StdIn.readLine()

    print("Does this node have dependencies? (y/n): ")
    val hasDeps = scala.io.StdIn.readLine().toLowerCase.startsWith("y")

    val node = new DummyNode().setName(nodeName)

    if (hasDeps) {
      print("Enter number of dependencies: ")
      val numDeps = scala.io.StdIn.readLine().toInt
      val deps = (1 to numDeps).map(_ => readNodeInput(nodeName))
      node.setDependencies(util.Arrays.asList(deps: _*))
    }

    node
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = createWorkflowClient()

    println("\nWorkflow Runner - DAG Execution")
    println("===============================")

    var continue = true
    while (continue) {
      printMenu()

      scala.io.StdIn.readLine() match {
        case "1" =>
          println("\nExecuting Simple DAG...")
          executeNode(client, getSimpleNode)

        case "2" =>
          println("\nExecuting Complex DAG...")
          executeNode(client, getComplexNode)

        case "3" =>
          println("\nCreating Custom DAG...")
          val customNode = readNodeInput()
          executeNode(client, customNode)

        case "4" =>
          println("\nExiting...")
          continue = false

        case _ =>
          println("\nInvalid option, please try again.")
      }
    }
  }
}
