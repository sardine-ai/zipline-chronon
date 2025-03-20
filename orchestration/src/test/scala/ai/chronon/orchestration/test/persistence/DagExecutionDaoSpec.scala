package ai.chronon.orchestration.test.persistence

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.persistence.{Dag, DagRootNode, DagExecutionDao, DagRunInfo}

import scala.concurrent.Await
import scala.concurrent.duration._

class DagExecutionDaoSpec extends BaseDaoSpec {
  // Create the DAO to test
  private lazy val dao = new DagExecutionDao(db)

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  // Sample data for tests
  private val range1 = PartitionRange("2023-01-01", "2023-01-31")
  private val range2 = PartitionRange("2023-02-01", "2023-02-28")

  // Sample DAGs
  private val dag1 = Dag(1L, "user1", "main", "abc123")
  private val dag2 = Dag(2L, "user2", "feature", "def456")
  private val dag3 = Dag(3L, "user1", "dev", "xyz789")

  // Sample root nodes
  private val rootNode1 = DagRootNode(1L, 101L)
  private val rootNode2 = DagRootNode(1L, 102L)
  private val rootNode3 = DagRootNode(2L, 201L)

  // Sample DAG run info
  private val dagRunInfo1 = DagRunInfo("run_001", 1L, 101L, "confId1", range1)
  private val dagRunInfo2 = DagRunInfo("run_001", 1L, 102L, "confId2", range1)
  private val dagRunInfo3 = DagRunInfo("run_002", 2L, 201L, "confId3", range2)

  /** Setup method called once before all tests
    */
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create tables and insert test data
    val setup = for {
      // Drop tables if they exist (cleanup from previous tests)
      _ <- dao.dropDagTableIfExists()
      _ <- dao.dropDagRootNodeTableIfExists()
      _ <- dao.dropDagRunInfoTableIfExists()

      // Create tables
      _ <- dao.createDagTableIfNotExists()
      _ <- dao.createDagRootNodeTableIfNotExists()
      _ <- dao.createDagRunInfoTableIfNotExists()

      // Insert test data
      _ <- dao.insertDags(Seq(dag1, dag2, dag3))
      _ <- dao.insertDagRootNodes(Seq(rootNode1, rootNode2, rootNode3))
      _ <- dao.insertDagRunInfoRecords(Seq(dagRunInfo1, dagRunInfo2, dagRunInfo3))
    } yield ()

    // Wait for setup to complete
    Await.result(setup, patience.timeout.toSeconds.seconds)
  }

  /** Cleanup method called once after all tests
    */
  override def afterAll(): Unit = {
    // Clean up database by dropping the tables
    val cleanup = for {
      _ <- dao.dropDagTableIfExists()
      _ <- dao.dropDagRootNodeTableIfExists()
      _ <- dao.dropDagRunInfoTableIfExists()
    } yield ()

    Await.result(cleanup, patience.timeout.toSeconds.seconds)

    // Let parent handle closing the connection
    super.afterAll()
  }

  // Shared test definitions
  "DagExecutionDao" should "get a DAG by ID" in {
    val dags = dao.getDagById(1L).futureValue
    dags should have size 1
    dags.head shouldBe dag1
  }

  it should "return empty list when dag_id doesn't exist" in {
    val dags = dao.getDagById(999L).futureValue
    dags shouldBe empty
  }

  it should "insert a new DAG" in {
    val newDag = Dag(4L, "user3", "test", "test123")
    val insertResult = dao.insertDag(newDag).futureValue
    insertResult shouldBe 1

    val retrievedDags = dao.getDagById(4L).futureValue
    retrievedDags should have size 1
    retrievedDags.head shouldBe newDag
  }

  it should "get all DAGs by user" in {
    val userDags = dao.getDagsByUser("user1").futureValue
    userDags should have size 2
    userDags.map(_.user).distinct shouldBe Seq("user1")
  }

  it should "delete a DAG by id" in {
    val deleteResult = dao.deleteDag(3L).futureValue
    deleteResult shouldBe 1

    val dags = dao.getDagById(3L).futureValue
    dags shouldBe empty
  }

  // Tests for root node functionality
  it should "get root node IDs for a DAG" in {
    val rootNodeIds = dao.getRootNodeIds(1L).futureValue
    rootNodeIds should contain theSameElementsAs Seq(101L, 102L)
  }

  // Tests for DAG run info functionality
  it should "get DAG run info for a specific run" in {
    val runInfo = dao.getDagRunInfo("run_001").futureValue
    runInfo should have size 2
    runInfo.map(_.nodeId) should contain theSameElementsAs Seq(101L, 102L)
  }

  it should "get DAG run info for a specific node in a run" in {
    val nodeRunInfo = dao.getDagRunInfoForNode("run_001", 101L).futureValue
    nodeRunInfo should have size 1
    nodeRunInfo.head.nodeId shouldBe 101L
  }
}
