package ai.chronon.orchestration.test.persistence

import ai.chronon.orchestration.persistence._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class NodeDaoSpec extends BaseDaoSpec {
  // Create the DAO to test
  private lazy val dao = new NodeDao(db)

  // Sample data for tests
  private val testBranch = "main"

  // Sample Nodes
  private val testNodes = Seq(
    Node("extract", testBranch, """{"type": "extraction"}""", "hash1", 1),
    Node("transform", testBranch, """{"type": "transformation"}""", "hash2", 1),
    Node("load", testBranch, """{"type": "loading"}""", "hash3", 1),
    Node("validate", testBranch, """{"type": "validation"}""", "hash4", 1)
  )

  // Sample Node dependencies
  private val testNodeDependencies = Seq(
    NodeDependency("extract", "transform", testBranch), // extract -> transform
    NodeDependency("transform", "load", testBranch), // transform -> load
    NodeDependency("transform", "validate", testBranch) // transform -> validate
  )

  // Sample Node runs
  private val testNodeRuns = Seq(
    NodeRun("run_001", "extract", testBranch, "2023-01-01", "2023-01-31", "COMPLETED"),
    NodeRun("run_002", "transform", testBranch, "2023-01-01", "2023-01-31", "RUNNING"),
    NodeRun("run_003", "load", testBranch, "2023-01-01", "2023-01-31", "PENDING"),
    NodeRun("run_004", "extract", testBranch, "2023-02-01", "2023-02-28", "COMPLETED")
  )

  // Sample NodeRunDependencies
  private val testNodeRunDependencies = Seq(
    NodeRunDependency("run_001", "run_002"),
    NodeRunDependency("run_002", "run_003")
  )

  // Sample NodeRunAttempts
  private val testNodeRunAttempts = Seq(
    NodeRunAttempt("run_001", "attempt_1", "2023-01-01T10:00:00", Some("2023-01-01T10:10:00"), "COMPLETED"),
    NodeRunAttempt("run_002", "attempt_1", "2023-01-01T10:15:00", None, "RUNNING")
  )

  /** Setup method called once before all tests
    */
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create tables and insert test data
    val setup = for {
      // Drop tables if they exist (cleanup from previous tests)
      _ <- dao.dropNodeRunAttemptTableIfExists()
      _ <- dao.dropNodeRunDependencyTableIfExists()
      _ <- dao.dropNodeDependencyTableIfExists()
      _ <- dao.dropNodeRunTableIfExists()
      _ <- dao.dropNodeTableIfExists()

      // Create tables
      _ <- dao.createNodeTableIfNotExists()
      _ <- dao.createNodeRunTableIfNotExists()
      _ <- dao.createNodeDependencyTableIfNotExists()
      _ <- dao.createNodeRunDependencyTableIfNotExists()
      _ <- dao.createNodeRunAttemptTableIfNotExists()

      // Insert test data
      _ <- Future.sequence(testNodes.map(dao.insertNode))
      _ <- Future.sequence(testNodeDependencies.map(dao.insertNodeDependency))
      _ <- Future.sequence(testNodeRuns.map(dao.insertNodeRun))
      _ <- Future.sequence(testNodeRunDependencies.map(dao.insertNodeRunDependency))
      _ <- Future.sequence(testNodeRunAttempts.map(dao.insertNodeRunAttempt))
    } yield ()

    // Wait for setup to complete
    Await.result(setup, patience.timeout.toSeconds.seconds)
  }

  /** Cleanup method called once after all tests
    */
  override def afterAll(): Unit = {
    // Clean up database by dropping the tables
    val cleanup = for {
      _ <- dao.dropNodeRunAttemptTableIfExists()
      _ <- dao.dropNodeRunDependencyTableIfExists()
      _ <- dao.dropNodeDependencyTableIfExists()
      _ <- dao.dropNodeRunTableIfExists()
      _ <- dao.dropNodeTableIfExists()
    } yield ()

    Await.result(cleanup, patience.timeout.toSeconds.seconds)

    // Let parent handle closing the connection
    super.afterAll()
  }

  // Node operations tests
  "NodeDao" should "get a Node by name and branch" in {
    val node = dao.getNode("extract", testBranch).futureValue
    node shouldBe defined
    node.get.nodeName shouldBe "extract"
    node.get.contentHash shouldBe "hash1"
  }

  it should "return None when node doesn't exist" in {
    val node = dao.getNode("nonexistent", testBranch).futureValue
    node shouldBe None
  }

  it should "insert a new Node" in {
    val newNode = Node("analyze", testBranch, """{"type": "analysis"}""", "hash5", 1)
    val insertResult = dao.insertNode(newNode).futureValue
    insertResult shouldBe 1

    val retrievedNode = dao.getNode("analyze", testBranch).futureValue
    retrievedNode shouldBe defined
    retrievedNode.get.nodeName shouldBe "analyze"
  }

  it should "update a Node" in {
    val node = dao.getNode("validate", testBranch).futureValue.get
    val updatedNode = node.copy(contentHash = "hash4-updated")

    val updateResult = dao.updateNode(updatedNode).futureValue
    updateResult shouldBe 1

    val retrievedNode = dao.getNode("validate", testBranch).futureValue
    retrievedNode shouldBe defined
    retrievedNode.get.contentHash shouldBe "hash4-updated"
  }

  // NodeRun tests
  it should "get NodeRun by run ID" in {
    val nodeRun = dao.getNodeRun("run_001").futureValue
    nodeRun shouldBe defined
    nodeRun.get.nodeName shouldBe "extract"
    nodeRun.get.status shouldBe "COMPLETED"
  }

  it should "update NodeRun status" in {
    val updateResult = dao.updateNodeRunStatus("run_002", "COMPLETED").futureValue
    updateResult shouldBe 1

    val nodeRun = dao.getNodeRun("run_002").futureValue
    nodeRun shouldBe defined
    nodeRun.get.status shouldBe "COMPLETED"
  }

  // NodeDependency tests
  it should "get child nodes" in {
    val childNodes = dao.getChildNodes("transform", testBranch).futureValue
    childNodes should contain theSameElementsAs Seq("load", "validate")
  }

  it should "get parent nodes" in {
    val parentNodes = dao.getParentNodes("transform", testBranch).futureValue
    parentNodes should contain only "extract"
  }

  it should "add a new dependency" in {
    val newDependency = NodeDependency("load", "validate", testBranch)
    val addResult = dao.insertNodeDependency(newDependency).futureValue
    addResult shouldBe 1

    val children = dao.getChildNodes("load", testBranch).futureValue
    children should contain only "validate"
  }

  // NodeRunDependency tests
  it should "get child node runs" in {
    val childRuns = dao.getChildNodeRuns("run_001").futureValue
    childRuns should contain only "run_002"
  }

  // NodeRunAttempt tests
  it should "get node run attempts by run ID" in {
    val attempts = dao.getNodeRunAttempts("run_001").futureValue
    attempts should have size 1
    attempts.head.attemptId shouldBe "attempt_1"
    attempts.head.status shouldBe "COMPLETED"
  }

  it should "update node run attempt status" in {
    val updateResult =
      dao.updateNodeRunAttemptStatus("run_002", "attempt_1", "2023-01-01T10:30:00", "COMPLETED").futureValue
    updateResult shouldBe 1

    val attempts = dao.getNodeRunAttempts("run_002").futureValue
    attempts should have size 1
    attempts.head.status shouldBe "COMPLETED"
    attempts.head.endTime shouldBe Some("2023-01-01T10:30:00")
  }
}
