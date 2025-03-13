package ai.chronon.orchestration.persistence

import ai.chronon.api.{PartitionRange, PartitionSpec}
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future

/** Data model classes for Node execution
  */
case class Node(nodeId: Long, nodeName: String, version: String, nodeType: String)

case class NodeRunInfo(
    nodeRunId: String,
    nodeId: Long,
    confId: String,
    partitionRange: PartitionRange,
    status: String
)

case class NodeDependsOnNode(parentNodeId: Long, childNodeId: Long)

/** Slick table definitions
  */
class NodeTable(tag: Tag) extends Table[Node](tag, "Node") {
  val nodeId = column[Long]("node_id", O.PrimaryKey)
  val nodeName = column[String]("node_name")
  val version = column[String]("version")
  val nodeType = column[String]("node_type")

  def * = (nodeId, nodeName, version, nodeType).mapTo[Node]
}

class NodeRunInfoTable(tag: Tag) extends Table[NodeRunInfo](tag, "NodeRunInfo") {
  val nodeRunId = column[String]("node_run_id", O.PrimaryKey)
  val nodeId = column[Long]("node_id")
  val confId = column[String]("conf_id")
  val partitionRangeStart = column[String]("partition_range_start")
  val partitionRangeEnd = column[String]("partition_range_end")
  val partitionSpecFormat = column[String]("partition_spec_format")
  val partitionSpecMillis = column[Long]("partition_spec_millis")
  val status = column[String]("status")

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

  def * = (nodeRunId, nodeId, confId, partitionRange, status).mapTo[NodeRunInfo]
}

class NodeDependsOnNodeTable(tag: Tag) extends Table[NodeDependsOnNode](tag, "NodeDependsOnNode") {
  val parentNodeId = column[Long]("parent_node_id")
  val childNodeId = column[Long]("child_node_id")

  def * = (parentNodeId, childNodeId).mapTo[NodeDependsOnNode]
}

/** DAO for Node execution operations
  */
class NodeExecutionDao(db: Database) {
  private val nodeTable = TableQuery[NodeTable]
  private val nodeRunInfoTable = TableQuery[NodeRunInfoTable]
  private val nodeDependencyTable = TableQuery[NodeDependsOnNodeTable]

  // Table creation methods
  def createNodeTableIfNotExists(): Future[Unit] = {
    db.run(nodeTable.schema.createIfNotExists)
  }

  def createNodeRunInfoTableIfNotExists(): Future[Unit] = {
    db.run(nodeRunInfoTable.schema.createIfNotExists)
  }

  def createNodeDependencyTableIfNotExists(): Future[Int] = {
    val createNodeDependencyTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeDependsOnNode" (
        "parent_node_id" BIGINT NOT NULL,
        "child_node_id" BIGINT NOT NULL,
        PRIMARY KEY("parent_node_id", "child_node_id")
      )
    """
    db.run(createNodeDependencyTableSQL)
  }

  // Table drop methods
  def dropNodeTableIfExists(): Future[Unit] = {
    db.run(nodeTable.schema.dropIfExists)
  }

  def dropNodeRunInfoTableIfExists(): Future[Unit] = {
    db.run(nodeRunInfoTable.schema.dropIfExists)
  }

  def dropNodeDependencyTableIfExists(): Future[Unit] = {
    db.run(nodeDependencyTable.schema.dropIfExists)
  }

  // Node operations
  def insertNode(node: Node): Future[Int] = {
    db.run(nodeTable += node)
  }

  def insertNodes(nodes: Seq[Node]): Future[Option[Int]] = {
    db.run(nodeTable ++= nodes)
  }

  def getNodeById(nodeId: Long): Future[Option[Node]] = {
    db.run(nodeTable.filter(_.nodeId === nodeId).result.headOption)
  }

  def deleteNode(nodeId: Long): Future[Int] = {
    db.run(nodeTable.filter(_.nodeId === nodeId).delete)
  }

  // NodeRunInfo operations
  def insertNodeRunInfo(nodeRunInfo: NodeRunInfo): Future[Int] = {
    db.run(nodeRunInfoTable += nodeRunInfo)
  }

  def insertNodeRunInfos(nodeRunInfos: Seq[NodeRunInfo]): Future[Option[Int]] = {
    db.run(nodeRunInfoTable ++= nodeRunInfos)
  }

  def getNodeRunInfo(nodeRunId: String): Future[Seq[NodeRunInfo]] = {
    db.run(nodeRunInfoTable.filter(_.nodeRunId === nodeRunId).result)
  }

  def getNodeRunInfoForNode(nodeRunId: String, nodeId: Long): Future[Option[NodeRunInfo]] = {
    db.run(
      nodeRunInfoTable
        .filter(r => r.nodeRunId === nodeRunId && r.nodeId === nodeId)
        .result
        .headOption
    )
  }

  def updateNodeRunStatus(nodeRunId: String, nodeId: Long, newStatus: String): Future[Int] = {
    val query = for {
      run <- nodeRunInfoTable if run.nodeRunId === nodeRunId && run.nodeId === nodeId
    } yield run.status

    db.run(query.update(newStatus))
  }

  // Node dependency operations
  def addNodeDependency(parentNodeId: Long, childNodeId: Long): Future[Int] = {
    db.run(nodeDependencyTable += NodeDependsOnNode(parentNodeId, childNodeId))
  }

  def removeNodeDependency(parentNodeId: Long, childNodeId: Long): Future[Int] = {
    db.run(
      nodeDependencyTable
        .filter(d => d.parentNodeId === parentNodeId && d.childNodeId === childNodeId)
        .delete
    )
  }

  def getChildNodes(parentNodeId: Long): Future[Seq[Long]] = {
    db.run(
      nodeDependencyTable
        .filter(_.parentNodeId === parentNodeId)
        .map(_.childNodeId)
        .result
    )
  }

  def getParentNodes(childNodeId: Long): Future[Seq[Long]] = {
    db.run(
      nodeDependencyTable
        .filter(_.childNodeId === childNodeId)
        .map(_.parentNodeId)
        .result
    )
  }
}
