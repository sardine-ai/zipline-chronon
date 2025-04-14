package ai.chronon.orchestration.test.persistence

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.NodeRunStatus
import ai.chronon.orchestration.persistence._
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import ai.chronon.orchestration.test.utils.TestUtils._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class NodeDaoSpec extends BaseDaoSpec {
  // Create the DAO to test
  private lazy val dao = new NodeDao(db)

  private val testBranch = Branch("test")
  private val stepDays = StepDays(1)

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  // Sample Nodes
  private val testNodes = Seq(
    Node(NodeName("extract"), """{"type": "extraction"}""", "hash1", stepDays),
    Node(NodeName("transform"), """{"type": "transformation"}""", "hash2", stepDays),
    Node(NodeName("load"), """{"type": "loading"}""", "hash3", stepDays),
    Node(NodeName("validate"), """{"type": "validation"}""", "hash4", stepDays)
  )

  // Sample NodeTableDependency objects
  private val testNodeTableDependencies = Seq(
    NodeTableDependency(
      NodeName("extract"),
      NodeName("transform"),
      createTestTableDependency("extract_data", Some("date"))
    ),
    NodeTableDependency(
      NodeName("transform"),
      NodeName("load"),
      createTestTableDependency("transformed_data", Some("dt"))
    ),
    NodeTableDependency(
      NodeName("transform"),
      NodeName("validate"),
      createTestTableDependency("validation_data")
    )
  )

  // Sample Node runs with the updated schema
  private val testNodeRuns = Seq(
    NodeRun(NodeName("extract"),
            "2023-01-01",
            "2023-01-31",
            "run_001",
            testBranch,
            "2023-01-01T10:00:00",
            Some("2023-01-01T10:10:00"),
            NodeRunStatus.SUCCEEDED),
    NodeRun(NodeName("transform"),
            "2023-01-01",
            "2023-01-31",
            "run_002",
            testBranch,
            "2023-01-01T10:15:00",
            None,
            NodeRunStatus.RUNNING),
    NodeRun(NodeName("load"),
            "2023-01-01",
            "2023-01-31",
            "run_003",
            testBranch,
            "2023-01-01T10:20:00",
            None,
            NodeRunStatus.WAITING),
    NodeRun(NodeName("extract"),
            "2023-02-01",
            "2023-02-28",
            "run_004",
            testBranch,
            "2023-02-01T10:00:00",
            Some("2023-02-01T10:30:00"),
            NodeRunStatus.SUCCEEDED)
  )

  /** Setup method called once before all tests
    */
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create tables and insert test data
    val setup = for {
      // Drop tables if they exist (cleanup from previous tests)
      _ <- dao.dropNodeTableDependencyTableIfExists()
      _ <- dao.dropNodeRunTableIfExists()
      _ <- dao.dropNodeTableIfExists()

      // Create tables
      _ <- dao.createNodeTableIfNotExists()
      _ <- dao.createNodeRunTableIfNotExists()
      _ <- dao.createNodeTableDependencyTableIfNotExists()

      // Insert test data
      _ <- Future.sequence(testNodes.map(dao.insertNode))
      _ <- Future.sequence(testNodeTableDependencies.map(dao.insertNodeTableDependency))
      _ <- Future.sequence(testNodeRuns.map(dao.insertNodeRun))
    } yield ()

    // Wait for setup to complete
    Await.result(setup, patience.timeout.toSeconds.seconds)
  }

  /** Cleanup method called once after all tests
    */
  override def afterAll(): Unit = {
    // Clean up database by dropping the tables
    val cleanup = for {
      _ <- dao.dropNodeTableDependencyTableIfExists()
      _ <- dao.dropNodeRunTableIfExists()
      _ <- dao.dropNodeTableIfExists()
    } yield ()

    Await.result(cleanup, patience.timeout.toSeconds.seconds)

    // Let parent handle closing the connection
    super.afterAll()
  }

  // Node operations tests
  "NodeDao" should "get a Node by name" in {
    val node = dao.getNode(NodeName("extract")).futureValue
    node shouldBe defined
    node.get.nodeName.name shouldBe "extract"
    node.get.contentHash shouldBe "hash1"
  }

  it should "return None when node doesn't exist" in {
    val node = dao.getNode(NodeName("nonexistent")).futureValue
    node shouldBe None
  }

  it should "insert a new Node" in {
    val newNode = Node(NodeName("analyze"), """{"type": "analysis"}""", "hash5", stepDays)
    val insertResult = dao.insertNode(newNode).futureValue
    insertResult shouldBe 1

    val retrievedNode = dao.getNode(NodeName("analyze")).futureValue
    retrievedNode shouldBe defined
    retrievedNode.get.nodeName.name shouldBe "analyze"
  }

  it should "update a Node" in {
    val node = dao.getNode(NodeName("validate")).futureValue.get
    val updatedNode = node.copy(contentHash = "hash4-updated")

    val updateResult = dao.updateNode(updatedNode).futureValue
    updateResult shouldBe 1

    val retrievedNode = dao.getNode(NodeName("validate")).futureValue
    retrievedNode shouldBe defined
    retrievedNode.get.contentHash shouldBe "hash4-updated"
  }

  // NodeRun tests
  it should "get NodeRun by run ID" in {
    val nodeRun = dao.getNodeRun("run_001").futureValue
    nodeRun shouldBe defined
    nodeRun.get.nodeName.name shouldBe "extract"
    nodeRun.get.status shouldBe NodeRunStatus.SUCCEEDED
    nodeRun.get.startTime shouldBe "2023-01-01T10:00:00"
    nodeRun.get.endTime shouldBe Some("2023-01-01T10:10:00")
  }

  it should "find latest NodeRun by node parameters" in {
    val nodeExecutionRequest = NodeExecutionRequest(
      NodeName("extract"),
      testBranch,
      PartitionRange("2023-01-01", "2023-01-31")
    )
    val nodeRun = dao.findLatestCoveringRun(nodeExecutionRequest).futureValue
    nodeRun shouldBe defined
    nodeRun.get.runId shouldBe "run_001"
    nodeRun.get.status shouldBe NodeRunStatus.SUCCEEDED
  }

  it should "update NodeRun status" in {
    val nodeRun = dao.getNodeRun("run_002").futureValue.get
    val updateTime = "2023-01-01T11:00:00"
    val updatedNodeRun = nodeRun.copy(endTime = Some(updateTime), status = NodeRunStatus.SUCCEEDED)

    val updateResult = dao.updateNodeRunStatus(updatedNodeRun).futureValue
    updateResult shouldBe 1

    val retrievedNodeRun = dao.getNodeRun("run_002").futureValue
    retrievedNodeRun shouldBe defined
    retrievedNodeRun.get.status shouldBe NodeRunStatus.SUCCEEDED
    retrievedNodeRun.get.endTime shouldBe Some(updateTime)
  }

  // NodeTableDependency tests
  it should "get child nodes" in {
    val childNodes = dao.getChildNodes(NodeName("transform")).futureValue
    childNodes should contain theSameElementsAs Seq(NodeName("load"), NodeName("validate"))
  }

  it should "add a new table dependency" in {
    val newDependency = NodeTableDependency(
      NodeName("load"),
      NodeName("validate"),
      createTestTableDependency("processed_data", Some("partition_dt"))
    )
    val addResult = dao.insertNodeTableDependency(newDependency).futureValue
    addResult shouldBe 1

    val children = dao.getChildNodes(NodeName("load")).futureValue
    children should contain only NodeName("validate")
  }

  it should "get NodeTableDependencies by parent node" in {
    val dependencies = dao.getNodeTableDependencies(NodeName("transform")).futureValue

    // Check if we have the correct number of dependencies
    dependencies.size shouldBe 2

    // Verify we got the expected child nodes
    val childNodeNames = dependencies.map(_.childNodeName)
    childNodeNames should contain theSameElementsAs Seq(NodeName("load"), NodeName("validate"))
  }

  it should "properly deserialize the JSON-serialized TableDependency" in {
    // This test verifies our custom JSON serialization/deserialization works for complex Thrift objects
    val originalDependency = testNodeTableDependencies.head

    // First retrieve the dependency from the database
    val dependencies = dao.getNodeTableDependencies(originalDependency.parentNodeName).futureValue
    val retrievedDep = dependencies.find(_.childNodeName == originalDependency.childNodeName).get

    // Verify core fields
    retrievedDep.parentNodeName shouldBe originalDependency.parentNodeName
    retrievedDep.childNodeName shouldBe originalDependency.childNodeName

    // Verify TableDependency fields
    val retrievedTableDep = retrievedDep.tableDependency
    val originalTableDep = originalDependency.tableDependency

    // Verify TableInfo
    val retrievedTableInfo = retrievedTableDep.getTableInfo
    retrievedTableInfo.getTable shouldBe originalTableDep.getTableInfo.getTable
    retrievedTableInfo.getPartitionColumn shouldBe originalTableDep.getTableInfo.getPartitionColumn

    // Verify Window (partition interval)
    retrievedTableInfo.getPartitionInterval.getLength shouldBe originalTableDep.getTableInfo.getPartitionInterval.getLength
    retrievedTableInfo.getPartitionInterval.getTimeUnit shouldBe originalTableDep.getTableInfo.getPartitionInterval.getTimeUnit
  }
}
