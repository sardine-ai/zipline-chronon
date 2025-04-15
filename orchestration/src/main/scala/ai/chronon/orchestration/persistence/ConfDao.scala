package ai.chronon.orchestration.persistence

import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database
import scala.concurrent.Future

/** Data model classes for Conf Repo
  */
case class Conf(confContents: String, confName: String, confHash: String)

class ConfTable(tag: Tag) extends Table[Conf](tag, "Conf") {

  val confHash = column[String]("conf_hash")
  val confName = column[String]("conf_name")
  val confContents = column[String]("conf_contents")

  def * = (confHash, confContents, confName).mapTo[Conf]
}

case class BranchToConf(branch: String, confName: String, confHash: String)

class BranchToConfTable(tag: Tag) extends Table[BranchToConf](tag, "BranchToConf") {

  val branch = column[String]("branch")
  val confName = column[String]("conf_name")
  val confHash = column[String]("conf_hash")

  def * = (branch, confName, confHash).mapTo[BranchToConf]
}

class ConfDao(db: Database) {
  private val confTable = TableQuery[ConfTable]

  // Method to create the `Conf` table if it doesn't exist
  def createConfTableIfNotExists(): Future[Int] = {
    val createConfTableSQL = sqlu"""
      CREATE TABLE IF NOT EXISTS "Conf" (
        "conf_hash" VARCHAR NOT NULL,
        "conf_name" VARCHAR NOT NULL,
        "conf_contents" VARCHAR NOT NULL,
        PRIMARY KEY("conf_name", "conf_hash")
      )
    """
    db.run(createConfTableSQL)
  }

  def dropConfTableIfExists(): Future[Unit] = {
    db.run(confTable.schema.dropIfExists)
  }

  // Method to insert a single Conf record
  def insertConf(conf: Conf): Future[Int] = {
    db.run(confTable += conf)
  }

  // Method to insert a seq of Conf record
  def insertConfs(confs: Seq[Conf]): Future[Option[Int]] = {
    db.run(confTable ++= confs)
  }

  // Method to get all confs by company, branch
  def getConfs: Future[Seq[Conf]] = {
    val query = confTable
    db.run(query.result)
  }

}
