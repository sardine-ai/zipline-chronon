package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.persistence.{Node, NodeDao}
import ai.chronon.orchestration.pubsub.{GcpPubSubConfig, PubSubAdmin, PubSubManager, PubSubPublisher, PubSubSubscriber}
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import ai.chronon.orchestration.temporal.activity.NodeExecutionActivityFactory
import ai.chronon.orchestration.temporal.constants.{
  NodeRangeCoordinatorWorkflowTaskQueue,
  NodeSingleStepWorkflowTaskQueue
}
import ai.chronon.orchestration.temporal.workflow.{
  NodeRangeCoordinatorWorkflowImpl,
  NodeSingleStepWorkflowImpl,
  WorkflowOperations
}
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import ai.chronon.orchestration.test.utils.TestUtils._
import ai.chronon.orchestration.utils.TemporalUtils
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Futures.PatienceConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import slick.jdbc.JdbcBackend.Database
import slick.util.AsyncExecutor

import scala.concurrent.duration.DurationLong
import scala.concurrent.{Await, ExecutionContext, Future}

/** This will trigger workflow runs on the local temporal server, so the pre-requisite would be to have the
  * temporal service running locally using `temporal server start-dev`
  *
  * For Pub/Sub testing, you also need:
  * 1. Start the Pub/Sub emulator: gcloud beta emulators pubsub start --project=test-project
  * 2. Set environment variable: export PUBSUB_EMULATOR_HOST=localhost:8085
  */
class NodeWorkflowIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Configure patience for ScalaFutures
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  // Add an implicit execution context
  implicit val ec: ExecutionContext = ExecutionContext.global

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val testBranch = Branch("test")

  // Pub/Sub test configuration
  private val projectId = "test-project"
  private val topicId = "test-topic"
  private val subscriptionId = "test-subscription"

  // Temporal variables
  private var workflowClient: WorkflowClient = _
  private var workflowOperations: WorkflowOperations = _
  private var factory: WorkerFactory = _

  // Spanner/Slick variables
  private val pgAdapterPort = 5432
  private var nodeDao: NodeDao = _

  // PubSub variables
  private var pubSubManager: PubSubManager = _
  private var publisher: PubSubPublisher = _
  private var subscriber: PubSubSubscriber = _
  private var admin: PubSubAdmin = _

  private val stepDays = StepDays(1)

  private val testNodes = Seq(
    Node(NodeName("root"), "nodeContents1", "hash1", stepDays),
    Node(NodeName("dep1"), "nodeContents2", "hash2", stepDays),
    Node(NodeName("dep2"), "nodeContents3", "hash3", stepDays),
    Node(NodeName("derivation"), "nodeContents4", "hash4", stepDays),
    Node(NodeName("join"), "nodeContents5", "hash5", stepDays),
    Node(NodeName("groupBy1"), "nodeContents6", "hash6", stepDays),
    Node(NodeName("groupBy2"), "nodeContents7", "hash7", stepDays),
    Node(NodeName("stagingQuery1"), "nodeContents8", "hash8", stepDays),
    Node(NodeName("stagingQuery2"), "nodeContents9", "hash9", stepDays)
  )

  private val testNodeTableDependencies = Seq(
    createTestNodeTableDependency("root", "dep1", "root_to_dep1_table"),
    createTestNodeTableDependency("root", "dep2", "root_to_dep2_table"),
    createTestNodeTableDependency("derivation", "join", "derivation_to_join_table"),
    createTestNodeTableDependency("join", "groupBy1", "join_to_groupBy1_table"),
    createTestNodeTableDependency("join", "groupBy2", "join_to_groupBy2_table"),
    createTestNodeTableDependency("groupBy1", "stagingQuery1", "groupBy1_to_stagingQuery1_table"),
    createTestNodeTableDependency("groupBy2", "stagingQuery2", "groupBy2_to_stagingQuery2_table")
  )

  override def beforeAll(): Unit = {
    // Set up Pub/Sub emulator resources
    setupPubSubResources()

    // Set up spanner resources
    setupSpannerResources()

    // Set up Temporal
    workflowClient = TemporalTestEnvironmentUtils.getLocalWorkflowClient
    workflowOperations = new WorkflowOperations(workflowClient)
    factory = WorkerFactory.newInstance(workflowClient)

    // Setup workers for node execution workflows
    val worker1 = factory.newWorker(NodeSingleStepWorkflowTaskQueue.toString)
    worker1.registerWorkflowImplementationTypes(classOf[NodeSingleStepWorkflowImpl])
    val worker2 = factory.newWorker(NodeRangeCoordinatorWorkflowTaskQueue.toString)
    worker2.registerWorkflowImplementationTypes(classOf[NodeRangeCoordinatorWorkflowImpl])

    // Create and register activity with PubSub configured
    val activity = NodeExecutionActivityFactory.create(workflowClient, nodeDao, publisher)
    worker1.registerActivitiesImplementations(activity)
    worker2.registerActivitiesImplementations(activity)

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

  private def setupSpannerResources(): Unit = {
    val db = Database.forURL(
      url = s"jdbc:postgresql://localhost:$pgAdapterPort/test-database",
      user = "",
      password = "",
      executor = AsyncExecutor("TestExecutor", numThreads = 5, queueSize = 100)
    )
    nodeDao = new NodeDao(db)
    // Create tables and insert test data
    val setup = for {
      // Drop tables if they exist (cleanup from previous tests)
      _ <- nodeDao.dropNodeTableIfExists()
      _ <- nodeDao.dropNodeRunTableIfExists()
      _ <- nodeDao.dropNodeTableDependencyTableIfExists()

      // Create tables
      _ <- nodeDao.createNodeTableIfNotExists()
      _ <- nodeDao.createNodeRunTableIfNotExists()
      _ <- nodeDao.createNodeTableDependencyTableIfNotExists()

      // Insert test data
      _ <- Future.sequence(testNodeTableDependencies.map(nodeDao.insertNodeTableDependency))
      _ <- Future.sequence(testNodes.map(nodeDao.insertNode))
    } yield ()

    // Wait for setup to complete
    Await.result(setup, patience.timeout.toSeconds.seconds)
  }

  override def afterAll(): Unit = {
    // Clean up Temporal resources
    if (factory != null) {
      factory.shutdown()
    }

    // Clean up Pub/Sub resources
    try {
      // This will shutdown all the necessary pubsub resources
      PubSubManager.shutdownAll()
    } catch {
      case e: Exception => println(s"Error during PubSub cleanup: ${e.getMessage}")
    }

    // Clean up database by dropping the tables
    val cleanup = for {
      _ <- nodeDao.dropNodeTableDependencyTableIfExists()
      _ <- nodeDao.dropNodeRunTableIfExists()
      _ <- nodeDao.dropNodeTableIfExists()
    } yield ()

    Await.result(cleanup, patience.timeout.toSeconds.seconds)
  }

  private def verifyAllNodeWorkflows(nodeRangeCoordinatorRequests: Seq[NodeExecutionRequest],
                                     nodeSingleStepRequests: Seq[NodeExecutionRequest],
                                     messagesSize: Int): Unit = {
    // Verify that all dependent node range coordinator workflows are started and finished successfully
    for (nodeRangeCoordinatorRequest <- nodeRangeCoordinatorRequests) {
      val workflowId = TemporalUtils.getNodeRangeCoordinatorWorkflowId(nodeRangeCoordinatorRequest)
      workflowOperations.getWorkflowStatus(workflowId) should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }

    // Verify that all dependent node step workflows are started and finished successfully
    for (nodeSingleStepRequest <- nodeSingleStepRequests) {
      val workflowId = TemporalUtils.getNodeSingleStepWorkflowId(nodeSingleStepRequest)
      workflowOperations.getWorkflowStatus(workflowId) should be(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)
    }

    // Verify Pub/Sub messages
    val messages = subscriber.pullMessages()

    // Verify we received the expected number of messages
    messages.size should be(messagesSize)
  }

  it should "handle simple node with one level deep correctly and publish messages to Pub/Sub" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("root"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-02")
    )

    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest).get()

    val nodeRangeCoordinatorRequests = Seq(
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-01", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    val nodeSingleStepRequests = Seq(
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("root"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("dep2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    // Verify that all node workflows are started and finished successfully
    verifyAllNodeWorkflows(nodeRangeCoordinatorRequests, nodeSingleStepRequests, 6)
  }

  it should "handle complex node with multiple levels deep correctly and publish messages to Pub/Sub" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("derivation"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-02")
    )
    // Trigger workflow and wait for it to complete
    workflowOperations.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest).get()

    // Define the expected workflows that should be executed
    val nodeRangeCoordinatorRequests = Seq(
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-01", "2023-01-02")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    val nodeSingleStepRequests = Seq(
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("derivation"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("join"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("groupBy2"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery1"), testBranch, PartitionRange("2023-01-02", "2023-01-02")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-01", "2023-01-01")),
      NodeExecutionRequest(NodeName("stagingQuery2"), testBranch, PartitionRange("2023-01-02", "2023-01-02"))
    )

    // Verify that all node workflows are started and finished successfully
    verifyAllNodeWorkflows(nodeRangeCoordinatorRequests, nodeSingleStepRequests, 12)
  }
}
