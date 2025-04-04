package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.pubsub.{PubSubAdmin, GcpPubSubConfig, PubSubManager, PubSubPublisher, PubSubSubscriber}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityFactory
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.workflow.{
  NodeExecutionWorkflowImpl,
  WorkflowOperations,
  WorkflowOperationsImpl
}
import ai.chronon.orchestration.test.utils.{TemporalTestEnvironmentUtils, TestNodeUtils}
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** This will trigger workflow runs on the local temporal server, so the pre-requisite would be to have the
  * temporal service running locally using `temporal server start-dev`
  *
  * For Pub/Sub testing, you also need:
  * 1. Start the Pub/Sub emulator: gcloud beta emulators pubsub start --project=test-project
  * 2. Set environment variable: export PUBSUB_EMULATOR_HOST=localhost:8085
  */
class NodeExecutionWorkflowIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Pub/Sub test configuration
  private val projectId = "test-project"
  private val topicId = "test-topic"
  private val subscriptionId = "test-subscription"

  // Temporal variables
  private var workflowClient: WorkflowClient = _
  private var workflowOperations: WorkflowOperations = _
  private var factory: WorkerFactory = _

  // PubSub variables
  private var pubSubManager: PubSubManager = _
  private var publisher: PubSubPublisher = _
  private var subscriber: PubSubSubscriber = _
  private var admin: PubSubAdmin = _

  override def beforeAll(): Unit = {
    // Set up Pub/Sub emulator resources
    setupPubSubResources()

    // Set up Temporal
    workflowClient = TemporalTestEnvironmentUtils.getLocalWorkflowClient
    workflowOperations = new WorkflowOperationsImpl(workflowClient)
    factory = WorkerFactory.newInstance(workflowClient)

    // Setup worker for node workflow execution
    val worker = factory.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])

    // Create and register activity with PubSub configured
    val activity = NodeExecutionActivityFactory.create(workflowClient, publisher)
    worker.registerActivitiesImplementations(activity)

    // Start all registered Workers. The Workers will start polling the Task Queue.
    factory.start()
  }

  private def setupPubSubResources(): Unit = {
    val emulatorHost = sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")
    val config = GcpPubSubConfig.forEmulator(projectId, emulatorHost)

    // Create necessary PubSub components
    pubSubManager = PubSubManager(config)
    admin = PubSubAdmin(config)

    // Create the topic and subscription
    admin.createTopic(topicId)
    admin.createSubscription(topicId, subscriptionId)

    // Get publisher and subscriber
    publisher = pubSubManager.getOrCreatePublisher(topicId)
    subscriber = pubSubManager.getOrCreateSubscriber(topicId, subscriptionId)
  }

  override def afterAll(): Unit = {
    // Clean up Temporal resources
    if (factory != null) {
      factory.shutdown()
    }

    // Clean up Pub/Sub resources
    try {
      publisher.shutdown()
      subscriber.shutdown()
      admin.close()
      pubSubManager.shutdown()

      // Also shutdown the manager to free all resources
      PubSubManager.shutdownAll()
    } catch {
      case e: Exception => println(s"Error during PubSub cleanup: ${e.getMessage}")
    }
  }

  it should "handle simple node with one level deep correctly and publish messages to Pub/Sub" in {
    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeWorkflow(TestNodeUtils.getSimpleNode).get()

    // Expected nodes
    val expectedNodes = Array("dep1", "dep2", "main")

    // Verify that all dependent node workflows are started and finished successfully
    for (dependentNode <- expectedNodes) {
      workflowOperations.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }

    // Verify Pub/Sub messages
    val messages = subscriber.pullMessages()

    // Verify we received the expected number of messages
    messages.size should be(expectedNodes.length)

    // Verify each node has a message
    val nodeNames = messages.map(_.getAttributes.getOrElse("nodeName", ""))
    nodeNames should contain allElementsOf (expectedNodes)
  }

  it should "handle complex node with multiple levels deep correctly and publish messages to Pub/Sub" in {
    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeWorkflow(TestNodeUtils.getComplexNode).get()

    // Expected nodes
    val expectedNodes = Array("StagingQuery1", "StagingQuery2", "GroupBy1", "GroupBy2", "Join", "Derivation")

    // Verify that all dependent node workflows are started and finished successfully
    for (dependentNode <- expectedNodes) {
      workflowOperations.getWorkflowStatus(s"node-execution-${dependentNode}") should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }

    // Verify Pub/Sub messages
    val messages = subscriber.pullMessages()

    // Verify we received the expected number of messages
    messages.size should be(expectedNodes.length)

    // Verify each node has a message
    val nodeNames = messages.map(_.getAttributes.getOrElse("nodeName", ""))
    nodeNames should contain allElementsOf (expectedNodes)
  }
}
