package ai.chronon.orchestration.test.persistence

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.persistence.{Node, NodeDependsOnNode, NodeExecutionDao, NodeRunInfo}

import scala.concurrent.Await
import scala.concurrent.duration._

class NodeExecutionDaoSpec extends BaseDaoSpec {
  // Create the DAO to test
  private lazy val dao = new NodeExecutionDao(db)

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  // Sample data for tests
  private val range1 = PartitionRange("2023-01-01", "2023-01-31")
  private val range2 = PartitionRange("2023-02-01", "2023-02-28")

  // Sample Nodes
  private val testNodes = Seq(
    Node(101L, "extract", "v1", "extraction"),
    Node(102L, "transform", "v1", "transformation"),
    Node(103L, "load", "v1", "loading"),
    Node(104L, "validate", "v1", "validation")
  )

  // Sample Node dependencies
  private val testNodeDependencies = Seq(
    NodeDependsOnNode(101L, 102L), // extract -> transform
    NodeDependsOnNode(102L, 103L), // transform -> load
    NodeDependsOnNode(102L, 104L) // transform -> validate
  )

  // Sample Node run info
  private val testNodeRunInfos = Seq(
    NodeRunInfo("run_001", 101L, "conf1", range1, "COMPLETED"),
    NodeRunInfo("run_002", 102L, "conf1", range1, "RUNNING"),
    NodeRunInfo("run_003", 103L, "conf1", range1, "PENDING"),
    NodeRunInfo("run_004", 101L, "conf2", range2, "COMPLETED")
  )

  /** Setup method called once before all tests
    */
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create tables and insert test data
    val setup = for {
      // Drop tables if they exist (cleanup from previous tests)
      _ <- dao.dropNodeTableIfExists()
      _ <- dao.dropNodeRunInfoTableIfExists()
      _ <- dao.dropNodeDependencyTableIfExists()

      // Create tables
      _ <- dao.createNodeTableIfNotExists()
      _ <- dao.createNodeRunInfoTableIfNotExists()
      _ <- dao.createNodeDependencyTableIfNotExists()

      // Insert test data
      _ <- dao.insertNodes(testNodes)
      _ <- dao.insertNodeRunInfos(testNodeRunInfos)
      _ <- dao.addNodeDependencies(testNodeDependencies)
    } yield ()

    // Wait for setup to complete
    Await.result(setup, patience.timeout.toSeconds.seconds)
  }

  /** Cleanup method called once after all tests
    */
  override def afterAll(): Unit = {
    // Clean up database by dropping the tables
    val cleanup = for {
      _ <- dao.dropNodeTableIfExists()
      _ <- dao.dropNodeRunInfoTableIfExists()
      _ <- dao.dropNodeDependencyTableIfExists()
    } yield ()

    Await.result(cleanup, patience.timeout.toSeconds.seconds)

    // Let parent handle closing the connection
    super.afterAll()
  }

  // Shared test definitions
  "NodeExecutionDao" should "get a Node by ID" in {
    val node = dao.getNodeById(101L).futureValue
    node shouldBe defined
    node.get.nodeName shouldBe "extract"
    node.get.nodeType shouldBe "extraction"
  }

  it should "return None when node_id doesn't exist" in {
    val node = dao.getNodeById(999L).futureValue
    node shouldBe None
  }

  it should "insert a new Node" in {
    val newNode = Node(105L, "analyze", "v1", "analysis")
    val insertResult = dao.insertNode(newNode).futureValue
    insertResult shouldBe 1

    val retrievedNode = dao.getNodeById(105L).futureValue
    retrievedNode shouldBe defined
    retrievedNode.get.nodeName shouldBe "analyze"
  }

  it should "delete a Node" in {
    val deleteResult = dao.deleteNode(104L).futureValue
    deleteResult shouldBe 1

    val retrievedNode = dao.getNodeById(104L).futureValue
    retrievedNode shouldBe None
  }

  // Node run info tests
  it should "get node run info by run ID" in {
    val runInfos = dao.getNodeRunInfo("run_001").futureValue
    runInfos should have size 1
    runInfos.head.nodeId shouldBe 101L
  }

  it should "get specific node run info" in {
    val runInfo = dao.getNodeRunInfoForNode("run_002", 102L).futureValue
    runInfo shouldBe defined
    runInfo.get.status shouldBe "RUNNING"
  }

  it should "update node run status" in {
    val updateResult = dao.updateNodeRunStatus("run_002", 102L, "COMPLETED").futureValue
    updateResult shouldBe 1

    val runInfo = dao.getNodeRunInfoForNode("run_002", 102L).futureValue
    runInfo shouldBe defined
    runInfo.get.status shouldBe "COMPLETED"
  }

  // Node dependency tests
  it should "get child nodes" in {
    val childNodes = dao.getChildNodes(102L).futureValue
    childNodes should contain theSameElementsAs Seq(103L, 104L)
  }

  it should "get parent nodes" in {
    val parentNodes = dao.getParentNodes(102L).futureValue
    parentNodes should contain only 101L
  }

  it should "add a new dependency" in {
    val addResult = dao.addNodeDependency(103L, 104L).futureValue
    addResult shouldBe 1

    val children = dao.getChildNodes(103L).futureValue
    children should contain only 104L
  }

  it should "remove a dependency" in {
    val removeResult = dao.removeNodeDependency(102L, 104L).futureValue
    removeResult shouldBe 1

    val children = dao.getChildNodes(102L).futureValue
    children should contain only 103L
  }
}
