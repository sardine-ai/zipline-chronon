package ai.chronon.orchestration.persistence

import ai.chronon.api.{PartitionRange, PartitionSpec}
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future

/** Data model classes for Dag execution
  */
case class Dag(dagId: Long, user: String, branch: String, sha: String)

case class DagRootNode(dagId: Long, rootNodeId: Long)

case class DagRunInfo(dagRunId: String, dagId: Long, nodeId: Long, confId: String, partitionRange: PartitionRange)

/** Slick table definitions
  */
class DagTable(tag: Tag) extends Table[Dag](tag, "Dag") {

  val dagId = column[Long]("dag_id", O.PrimaryKey)
  val user = column[String]("user")
  val branch = column[String]("branch")
  val sha = column[String]("sha")

  def * = (dagId, user, branch, sha).mapTo[Dag]
}

class DagRootNodeTable(tag: Tag) extends Table[DagRootNode](tag, "DagRootNode") {

  val dagId = column[Long]("dag_id")
  val rootNodeId = column[Long]("root_node_id")

  def * = (dagId, rootNodeId).mapTo[DagRootNode]
}

class DagRunInfoTable(tag: Tag) extends Table[DagRunInfo](tag, "DagRunInfo") {

  val dagRunId = column[String]("dag_run_id")
  val dagId = column[Long]("dag_id")
  val nodeId = column[Long]("node_id")
  val configId = column[String]("config_id")
  val partitionRangeStart = column[String]("partition_range_start")
  val partitionRangeEnd = column[String]("partition_range_end")
  val partitionSpecFormat = column[String]("partition_spec_format")
  val partitionSpecMillis = column[Long]("partition_spec_millis")

  // Bidirectional mapping from partition range to raw related fields stored in database
  def partitionRange = (partitionRangeStart, partitionRangeEnd, partitionSpecFormat, partitionSpecMillis) <> (
    (partitionInfo: (String, String, String, Long)) => {
      implicit val partitionSpec: PartitionSpec = PartitionSpec(partitionInfo._3, partitionInfo._4)
      PartitionRange(partitionInfo._1, partitionInfo._2)
    },
    (partitionRange: PartitionRange) => {
      Some(
        (partitionRange.start,
         partitionRange.end,
         partitionRange.partitionSpec.format,
         partitionRange.partitionSpec.spanMillis))
    }
  )

  def * = (dagRunId, dagId, nodeId, configId, partitionRange).mapTo[DagRunInfo]
}

/** DAO for Dag execution operations
  */
class DagExecutionDao(db: Database) {
  private val dagTable = TableQuery[DagTable]
  private val dagRootNodeTable = TableQuery[DagRootNodeTable]
  private val dagRunInfoTable = TableQuery[DagRunInfoTable]

  // Method to create the `Dag` table if it doesn't exist
  def createDagTableIfNotExists(): Future[Unit] = {
    db.run(dagTable.schema.createIfNotExists)
  }

  // Method to create the `DagRootNode` table if it doesn't exist
  def createDagRootNodeTableIfNotExists(): Future[Int] = {

    /** Using custom sql for create table statement as slick only supports specifying composite primary key
      * with a separate alter table command which is not working well with Spanner postgres support.
      * It will also be helpful going forward with spanner specific options like interleaving support etc
      */
    val createDagRootNodeTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "DagRootNode" (
        "dag_id" BIGINT NOT NULL,
        "root_node_id" BIGINT NOT NULL,
        PRIMARY KEY("dag_id", "root_node_id")
      )
    """

    db.run(createDagRootNodeTableSQL)
  }

  // Method to create the `DagRunInfo` table if it doesn't exist
  def createDagRunInfoTableIfNotExists(): Future[Int] = {

    /** Using custom sql for create table statement as slick only supports specifying composite primary key
      * with a separate alter table command which is not working well with Spanner postgres support.
      * It will also be helpful going forward with spanner specific options like interleaving support etc
      */
    val createDagRunInfoTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "DagRunInfo" (
        "dag_run_id" VARCHAR NOT NULL,
        "dag_id" BIGINT NOT NULL,
        "node_id" BIGINT NOT NULL,
        "config_id" VARCHAR,
        "partition_range_start" VARCHAR,
        "partition_range_end" VARCHAR,
        "partition_spec_format" VARCHAR,
        "partition_spec_millis" BIGINT,
        PRIMARY KEY(dag_run_id, "dag_id", "node_id")
      )
    """

    db.run(createDagRunInfoTableSQL)
  }

  // Method to drop the `Dag` table if it exists
  def dropDagTableIfExists(): Future[Unit] = {
    db.run(dagTable.schema.dropIfExists)
  }

  // Method to drop the `Dag` table if it exists
  def dropDagRootNodeTableIfExists(): Future[Unit] = {
    db.run(dagRootNodeTable.schema.dropIfExists)
  }

  // Method to drop the `Dag` table if it exists
  def dropDagRunInfoTableIfExists(): Future[Unit] = {
    db.run(dagRunInfoTable.schema.dropIfExists)
  }

  // Method to insert a single DagInfo record
  def insertDag(dag: Dag): Future[Int] = {
    db.run(dagTable += dag)
  }

  // Method to insert multiple DagInfo records in a batch
  def insertDags(dagSeq: Seq[Dag]): Future[Option[Int]] = {
    db.run(dagTable ++= dagSeq)
  }

  // Method to get Dag record for a given dag_id
  def getDagById(dagId: Long): Future[Seq[Dag]] = {
    val query = dagTable.filter(_.dagId === dagId)
    db.run(query.result)
  }

  // Method to delete a DAG by id
  def deleteDag(dagId: Long): Future[Int] = {
    val query = dagTable.filter(_.dagId === dagId).delete
    db.run(query)
  }

  // Method to get all DAGs by user
  def getDagsByUser(user: String): Future[Seq[Dag]] = {
    val query = dagTable.filter(_.user === user)
    db.run(query.result)
  }

  // Method to insert multiple root nodes for a dag in a batch
  def insertDagRootNodes(dagRootNodes: Seq[DagRootNode]): Future[Option[Int]] = {
    db.run(dagRootNodeTable ++= dagRootNodes)
  }

  // Method to get all root node ids for a dag
  def getRootNodeIds(dagId: Long): Future[Seq[Long]] = {
    val query = dagRootNodeTable
      .filter(_.dagId === dagId)
      .map(_.rootNodeId)

    db.run(query.result)
  }

  // Method to insert dag run info records in a batch
  def insertDagRunInfoRecords(dagRunInfoRecords: Seq[DagRunInfo]): Future[Option[Int]] = {
    db.run(dagRunInfoTable ++= dagRunInfoRecords)
  }

  // Method to get all node run info for a given dag run
  def getDagRunInfo(dagRunId: String): Future[Seq[DagRunInfo]] = {
    val query = dagRunInfoTable
      .filter(_.dagRunId === dagRunId)

    db.run(query.result)
  }

  // Method to get all node run statuses for a given dag run
  def getDagRunInfoForNode(dagRunId: String, nodeId: Long): Future[Seq[DagRunInfo]] = {
    val query = dagRunInfoTable
      .filter(_.dagRunId === dagRunId)
      .filter(_.nodeId === nodeId)

    db.run(query.result)
  }
}
