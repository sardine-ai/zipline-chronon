package ai.chronon.orchestration.persistence

import ai.chronon.api.TableDependency
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database
import ai.chronon.orchestration.temporal.CustomSlickColumnTypes._
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.TDeserializer
import ai.chronon.api.thrift.protocol.TJSONProtocol
import ai.chronon.orchestration.NodeRunStatus

import java.util.Base64
import scala.concurrent.Future

/** Data Access Layer for Node operations.
  *
  * This module provides database access for nodes, node runs, and node table dependencies,
  * using the Slick ORM for PostgresSQL. It includes table definitions and CRUD operations
  * for each entity type.
  *
  * The main entities are:
  * - Node: Represents a processing node in the computation graph
  * - NodeRun: Tracks execution of a node over a specific time range
  * - NodeTableDependency: Tracks parent-child relationships between nodes with table metadata
  *
  * This DAO layer abstracts database operations, returning Futures for non-blocking
  * database interactions. It includes methods to create required tables, insert/update
  * records, and query node metadata and relationships.
  */

/** Represents a processing node in the computation graph. */
case class Node(nodeName: NodeName, nodeContents: String, contentHash: String, stepDays: StepDays)

/** Represents an execution run of a node over a specific time range.
  *
  * A NodeRun is uniquely identified by the combination of
  * (nodeName, startPartition, endPartition, runId), allowing multiple
  * runs of the same node over different time ranges and run attempts.
  */
case class NodeRun(
    nodeName: NodeName,
    startPartition: String,
    endPartition: String,
    runId: String,
    branch: Branch,
    startTime: String,
    endTime: Option[String],
    status: NodeRunStatus
)

/** Represents a table dependency relationship between two nodes.
  *
  * A table dependency is uniquely identified by the combination of
  * (parentNodeName, childNodeName). It carries rich metadata through the TableDependency
  * Thrift object
  *
  * The TableDependency object is serialized to JSON for storage, allowing for
  * schema evolution and backward compatibility.
  */
case class NodeTableDependency(parentNodeName: NodeName, childNodeName: NodeName, tableDependency: TableDependency)

/** Slick table definitions for database schema mapping.
  *
  * These class definitions map our domain models to database tables:
  * - NodeTable: Maps the Node case class to the Node table
  * - NodeRunTable: Maps the NodeRun case class to the NodeRun table
  * - NodeTableDependencyTable: Maps the NodeTableDependency case class to the NodeTableDependency table
  */
class NodeTable(tag: Tag) extends Table[Node](tag, "Node") {

  val nodeName = column[NodeName]("node_name")
  val nodeContents = column[String]("node_contents")
  val contentHash = column[String]("content_hash")
  val stepDays = column[StepDays]("step_days")

  def * = (nodeName, nodeContents, contentHash, stepDays).mapTo[Node]
}

class NodeRunTable(tag: Tag) extends Table[NodeRun](tag, "NodeRun") {

  val nodeName = column[NodeName]("node_name")
  val startPartition = column[String]("start")
  val endPartition = column[String]("end")
  val runId = column[String]("run_id")
  val branch = column[Branch]("branch")
  val startTime = column[String]("start_time")
  val endTime = column[Option[String]]("end_time")
  val status = column[NodeRunStatus]("status")

  // Mapping to case class
  def * = (nodeName, startPartition, endPartition, runId, branch, startTime, endTime, status).mapTo[NodeRun]
}

class NodeTableDependencyTable(tag: Tag) extends Table[NodeTableDependency](tag, "NodeTableDependency") {

  // Node relationship columns - these uniquely identify the relationship
  val parentNodeName = column[NodeName]("parent_node_name")
  val childNodeName = column[NodeName]("child_node_name")

  // TableDependency stored as a JSON string - this allows for schema evolution
  private val tableDependencyJson = column[String]("table_dependency_json")

  // Helper method to serialize TableDependency to JSON string
  private def serializeTableDependency(tableDependency: TableDependency): String = {
    try {
      val serializer = new TSerializer(new TJSONProtocol.Factory())
      val bytes = serializer.serialize(tableDependency)
      Base64.getEncoder.encodeToString(bytes)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to serialize TableDependency: ${e.getMessage}", e)
    }
  }

  // Helper method to deserialize JSON string to TableDependency
  private def deserializeTableDependency(json: String): TableDependency = {
    try {
      val bytes = Base64.getDecoder.decode(json)
      val deserializer = new TDeserializer(new TJSONProtocol.Factory())
      val tableDependency = new TableDependency()
      deserializer.deserialize(tableDependency, bytes)
      tableDependency
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to deserialize TableDependency from JSON: ${e.getMessage}", e)
    }
  }

  // Bidirectional mapping from JSON string to TableDependency and back
  private def tableDependency = tableDependencyJson <> (
    deserializeTableDependency,
    (td: TableDependency) => Some(serializeTableDependency(td))
  )

  // Column mapping to case class
  def * = (parentNodeName, childNodeName, tableDependency).mapTo[NodeTableDependency]
}

class NodeDao(db: Database) {
  private val nodeTable = TableQuery[NodeTable]
  private val nodeRunTable = TableQuery[NodeRunTable]
  private val nodeTableDependencyTable = TableQuery[NodeTableDependencyTable]

  def createNodeTableIfNotExists(): Future[Int] = {
    val createNodeTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "Node" (
        "node_name" VARCHAR NOT NULL,
        "node_contents" VARCHAR NOT NULL,
        "content_hash" VARCHAR NOT NULL,
        "step_days" INT NOT NULL,
        PRIMARY KEY("node_name")
      )
    """
    db.run(createNodeTableSQL)
  }

  def createNodeRunTableIfNotExists(): Future[Int] = {
    val createNodeRunTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeRun" (
        "node_name" VARCHAR NOT NULL,
        "start" VARCHAR NOT NULL,
        "end" VARCHAR NOT NULL,
        "run_id" VARCHAR NOT NULL,
        "branch" VARCHAR NOT NULL,
        "start_time" VARCHAR NOT NULL,
        "end_time" VARCHAR,
        "status" VARCHAR NOT NULL,
        PRIMARY KEY("node_name", "start", "end", "run_id")
      )
    """
    db.run(createNodeRunTableSQL)
  }

  def createNodeTableDependencyTableIfNotExists(): Future[Int] = {
    val createNodeTableDependencyTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeTableDependency" (
        "parent_node_name" VARCHAR NOT NULL,
        "child_node_name" VARCHAR NOT NULL,
        "table_dependency_json" TEXT NOT NULL,
        PRIMARY KEY("parent_node_name", "child_node_name")
      )
    """
    db.run(createNodeTableDependencyTableSQL)
  }

  // Drop table methods using schema.dropIfExists
  def dropNodeTableIfExists(): Future[Unit] = {
    db.run(nodeTable.schema.dropIfExists)
  }

  def dropNodeRunTableIfExists(): Future[Unit] = {
    db.run(nodeRunTable.schema.dropIfExists)
  }

  def dropNodeTableDependencyTableIfExists(): Future[Unit] = {
    db.run(nodeTableDependencyTable.schema.dropIfExists)
  }

  // Node operations
  def insertNode(node: Node): Future[Int] = {
    db.run(nodeTable += node)
  }

  def getNode(nodeName: NodeName): Future[Option[Node]] = {
    db.run(nodeTable.filter(n => n.nodeName === nodeName).result.headOption)
  }

  def getStepDays(nodeName: NodeName): Future[StepDays] = {
    db.run(nodeTable.filter(n => n.nodeName === nodeName).map(_.stepDays).result.head)
  }

  def updateNode(node: Node): Future[Int] = {
    db.run(
      nodeTable
        .filter(n => n.nodeName === node.nodeName)
        .update(node)
    )
  }

  // NodeRun operations
  def insertNodeRun(nodeRun: NodeRun): Future[Int] = {
    db.run(nodeRunTable += nodeRun)
  }

  def getNodeRun(runId: String): Future[Option[NodeRun]] = {
    db.run(nodeRunTable.filter(_.runId === runId).result.headOption)
  }

  def findLatestCoveringRun(nodeExecutionRequest: NodeExecutionRequest): Future[Option[NodeRun]] = {
    // Find the latest covering run (by startTime) for the given node parameters
    db.run(
      nodeRunTable
        .filter(run =>
          run.nodeName === nodeExecutionRequest.nodeName &&
            run.startPartition <= nodeExecutionRequest.partitionRange.start &&
            run.endPartition >= nodeExecutionRequest.partitionRange.end)
        .sortBy(_.startTime.desc) // latest first
        .result
        .headOption
    )
  }

  private def isNodeRunOverlappingWithRequest(nodeRun: NodeRunTable,
                                              nodeExecutionRequest: NodeExecutionRequest): Rep[Boolean] = {
    // overlap detection logic
    val requestStartInRun =
      nodeRun.startPartition <= nodeExecutionRequest.partitionRange.start &&
        nodeRun.endPartition >= nodeExecutionRequest.partitionRange.start
    val runStartInRequest =
      nodeRun.startPartition >= nodeExecutionRequest.partitionRange.start &&
        nodeRun.startPartition <= nodeExecutionRequest.partitionRange.end

    requestStartInRun || runStartInRequest
  }

  def findOverlappingNodeRuns(nodeExecutionRequest: NodeExecutionRequest): Future[Seq[NodeRun]] = {
    db.run(
      nodeRunTable
        .filter(run =>
          run.nodeName === nodeExecutionRequest.nodeName &&
            isNodeRunOverlappingWithRequest(run, nodeExecutionRequest))
        .result
    )
  }

  def updateNodeRunStatus(updatedNodeRun: NodeRun): Future[Int] = {
    val query = for {
      run <- nodeRunTable if (
        run.nodeName === updatedNodeRun.nodeName &&
          run.startPartition === updatedNodeRun.startPartition &&
          run.endPartition === updatedNodeRun.endPartition &&
          run.runId === updatedNodeRun.runId
      )
    } yield (run.status, run.endTime)

    db.run(query.update((updatedNodeRun.status, updatedNodeRun.endTime)))
  }

  // NodeTableDependency operations
  def insertNodeTableDependency(dependency: NodeTableDependency): Future[Int] = {
    db.run(nodeTableDependencyTable += dependency)
  }

  def getNodeTableDependencies(parentNodeName: NodeName): Future[Seq[NodeTableDependency]] = {
    db.run(
      nodeTableDependencyTable
        .filter(dep => dep.parentNodeName === parentNodeName)
        .result
    )
  }

  def getChildNodes(parentNodeName: NodeName): Future[Seq[NodeName]] = {
    db.run(
      nodeTableDependencyTable
        .filter(dep => dep.parentNodeName === parentNodeName)
        .map(_.childNodeName)
        .result
    )
  }
}
