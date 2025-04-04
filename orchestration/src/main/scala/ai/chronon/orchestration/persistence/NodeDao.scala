package ai.chronon.orchestration.persistence

import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future

case class Node(nodeName: String, branch: String, nodeContents: String, contentHash: String, stepDays: Int)

case class NodeRun(runId: String, nodeName: String, branch: String, start: String, end: String, status: String)

case class NodeDependency(parentNodeName: String, childNodeName: String, branch: String)

case class NodeRunDependency(parentRunId: String, childRunId: String)

case class NodeRunAttempt(runId: String, attemptId: String, startTime: String, endTime: Option[String], status: String)

/** Slick table definitions
  */
class NodeTable(tag: Tag) extends Table[Node](tag, "Node") {
  val nodeName = column[String]("node_name")
  val branch = column[String]("branch")
  val nodeContents = column[String]("node_contents")
  val contentHash = column[String]("content_hash")
  val stepDays = column[Int]("step_days")

  def * = (nodeName, branch, nodeContents, contentHash, stepDays).mapTo[Node]
}

class NodeRunTable(tag: Tag) extends Table[NodeRun](tag, "NodeRun") {
  val runId = column[String]("run_id", O.PrimaryKey)
  val nodeName = column[String]("node_name")
  val branch = column[String]("branch")
  val start = column[String]("start")
  val end = column[String]("end")
  val status = column[String]("status")

  def * = (runId, nodeName, branch, start, end, status).mapTo[NodeRun]
}

class NodeDependencyTable(tag: Tag) extends Table[NodeDependency](tag, "NodeDependency") {
  val parentNodeName = column[String]("parent_node_name")
  val childNodeName = column[String]("child_node_name")
  val branch = column[String]("branch")

  def * = (parentNodeName, childNodeName, branch).mapTo[NodeDependency]
}

class NodeRunDependencyTable(tag: Tag) extends Table[NodeRunDependency](tag, "NodeRunDependency") {
  val parentRunId = column[String]("parent_run_id")
  val childRunId = column[String]("child_run_id")

  def * = (parentRunId, childRunId).mapTo[NodeRunDependency]
}

class NodeRunAttemptTable(tag: Tag) extends Table[NodeRunAttempt](tag, "NodeRunAttempt") {
  val runId = column[String]("run_id")
  val attemptId = column[String]("attempt_id")
  val startTime = column[String]("start_time")
  val endTime = column[Option[String]]("end_time")
  val status = column[String]("status")

  def * = (runId, attemptId, startTime, endTime, status).mapTo[NodeRunAttempt]
}

/** DAO for Node operations
  */
class NodeDao(db: Database) {
  private val nodeTable = TableQuery[NodeTable]
  private val nodeRunTable = TableQuery[NodeRunTable]
  private val nodeDependencyTable = TableQuery[NodeDependencyTable]
  private val nodeRunDependencyTable = TableQuery[NodeRunDependencyTable]
  private val nodeRunAttemptTable = TableQuery[NodeRunAttemptTable]

  def createNodeTableIfNotExists(): Future[Int] = {
    val createNodeTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "Node" (
        "node_name" VARCHAR NOT NULL,
        "branch" VARCHAR NOT NULL,
        "node_contents" VARCHAR NOT NULL,
        "content_hash" VARCHAR NOT NULL,
        "step_days" INT NOT NULL,
        PRIMARY KEY("node_name", "branch")
      )
    """
    db.run(createNodeTableSQL)
  }

  def createNodeRunTableIfNotExists(): Future[Int] = {
    val createNodeRunTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeRun" (
        "run_id" VARCHAR NOT NULL,
        "node_name" VARCHAR NOT NULL,
        "branch" VARCHAR NOT NULL,
        "start" VARCHAR NOT NULL,
        "end" VARCHAR NOT NULL,
        "status" VARCHAR NOT NULL,
        PRIMARY KEY("run_id")
      )
    """
    db.run(createNodeRunTableSQL)
  }

  def createNodeDependencyTableIfNotExists(): Future[Int] = {
    val createNodeDependencyTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeDependency" (
        "parent_node_name" VARCHAR NOT NULL,
        "child_node_name" VARCHAR NOT NULL,
        "branch" VARCHAR NOT NULL,
        PRIMARY KEY("parent_node_name", "child_node_name", "branch")
      )
    """
    db.run(createNodeDependencyTableSQL)
  }

  def createNodeRunDependencyTableIfNotExists(): Future[Int] = {
    val createNodeRunDependencyTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeRunDependency" (
        "parent_run_id" VARCHAR NOT NULL,
        "child_run_id" VARCHAR NOT NULL,
        PRIMARY KEY("parent_run_id", "child_run_id")
      )
    """
    db.run(createNodeRunDependencyTableSQL)
  }

  def createNodeRunAttemptTableIfNotExists(): Future[Int] = {
    val createNodeRunAttemptTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "NodeRunAttempt" (
        "run_id" VARCHAR NOT NULL,
        "attempt_id" VARCHAR NOT NULL,
        "start_time" VARCHAR NOT NULL,
        "end_time" VARCHAR,
        "status" VARCHAR NOT NULL,
        PRIMARY KEY("run_id", "attempt_id")
      )
    """
    db.run(createNodeRunAttemptTableSQL)
  }

  // Drop table methods using schema.dropIfExists
  def dropNodeTableIfExists(): Future[Unit] = {
    db.run(nodeTable.schema.dropIfExists)
  }

  def dropNodeRunTableIfExists(): Future[Unit] = {
    db.run(nodeRunTable.schema.dropIfExists)
  }

  def dropNodeDependencyTableIfExists(): Future[Unit] = {
    db.run(nodeDependencyTable.schema.dropIfExists)
  }

  def dropNodeRunDependencyTableIfExists(): Future[Unit] = {
    db.run(nodeRunDependencyTable.schema.dropIfExists)
  }

  def dropNodeRunAttemptTableIfExists(): Future[Unit] = {
    db.run(nodeRunAttemptTable.schema.dropIfExists)
  }

  // Node operations
  def insertNode(node: Node): Future[Int] = {
    db.run(nodeTable += node)
  }

  def getNode(nodeName: String, branch: String): Future[Option[Node]] = {
    db.run(nodeTable.filter(n => n.nodeName === nodeName && n.branch === branch).result.headOption)
  }

  def updateNode(node: Node): Future[Int] = {
    db.run(
      nodeTable
        .filter(n => n.nodeName === node.nodeName && n.branch === node.branch)
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

  def updateNodeRunStatus(runId: String, newStatus: String): Future[Int] = {
    val query = for {
      run <- nodeRunTable if run.runId === runId
    } yield run.status

    db.run(query.update(newStatus))
  }

  // NodeDependency operations
  def insertNodeDependency(dependency: NodeDependency): Future[Int] = {
    db.run(nodeDependencyTable += dependency)
  }

  def getChildNodes(parentNodeName: String, branch: String): Future[Seq[String]] = {
    db.run(
      nodeDependencyTable
        .filter(dep => dep.parentNodeName === parentNodeName && dep.branch === branch)
        .map(_.childNodeName)
        .result
    )
  }

  def getParentNodes(childNodeName: String, branch: String): Future[Seq[String]] = {
    db.run(
      nodeDependencyTable
        .filter(dep => dep.childNodeName === childNodeName && dep.branch === branch)
        .map(_.parentNodeName)
        .result
    )
  }

  // NodeRunDependency operations
  def insertNodeRunDependency(dependency: NodeRunDependency): Future[Int] = {
    db.run(nodeRunDependencyTable += dependency)
  }

  def getChildNodeRuns(parentRunId: String): Future[Seq[String]] = {
    db.run(
      nodeRunDependencyTable
        .filter(_.parentRunId === parentRunId)
        .map(_.childRunId)
        .result
    )
  }

  // NodeRunAttempt operations
  def insertNodeRunAttempt(attempt: NodeRunAttempt): Future[Int] = {
    db.run(nodeRunAttemptTable += attempt)
  }

  def getNodeRunAttempts(runId: String): Future[Seq[NodeRunAttempt]] = {
    db.run(nodeRunAttemptTable.filter(_.runId === runId).result)
  }

  def updateNodeRunAttemptStatus(runId: String, attemptId: String, endTime: String, newStatus: String): Future[Int] = {
    val query = for {
      attempt <- nodeRunAttemptTable if attempt.runId === runId && attempt.attemptId === attemptId
    } yield (attempt.endTime, attempt.status)

    db.run(query.update((Some(endTime), newStatus)))
  }
}
