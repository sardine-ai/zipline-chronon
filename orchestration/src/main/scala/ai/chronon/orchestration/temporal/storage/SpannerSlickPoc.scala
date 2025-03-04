package ai.chronon.orchestration.temporal.storage

import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._

import java.util.Properties

object SpannerSlickPoc {

  case class User(id: Long, firstName: String, lastName: String)

  class UserTable(tag: Tag) extends Table[User](tag, None, "User") {
    override def * = (id, firstName, lastName).mapTo[User]
    val id: Rep[Long] = column[Long]("id", O.PrimaryKey)
    val firstName: Rep[String] = column[String]("first_name")
    val lastName: Rep[String] = column[String]("last_name")

    // Define your secondary index on the lastName column
    def lastNameIndex = index("idx_users_last_name", lastName)
  }

  def main(args: Array[String]): Unit = {
    // Configure connection properties
    val connectionProps = new Properties()
    connectionProps.setProperty("autocommit", "true")

    /** https://cloud.google.com/spanner/docs/pgadapter-emulator has instructions on connecting PGAdapter to the
      * Spanner emulator. Combined docker container is the easier option
      * Note: docker run command might not work as is on M4 mac, and we need to add the following option
      * `--platform linux/amd64`
      */
    val db = Database.forURL(
      url = "jdbc:postgresql://localhost:5432/test-database",
      driver = "org.postgresql.Driver",
      prop = connectionProps,
      executor = AsyncExecutor("spanner-executor", numThreads = 5, queueSize = 1000)
    )

    val users = TableQuery[UserTable]

    // First check if table exists, create if it doesn't
    val createTableIfNotExists = users.schema.createIfNotExists

    try {
      // Create table with longer timeout
      println("Attempting to create table if not exists...")
      Await.result(db.run(createTableIfNotExists), 5.seconds)
      println("Table created successfully")

      // Insert operations with proper error handling
      val insertOps = DBIO
        .seq(
          users += User(1L, "John", "Doe"),
          users += User(2L, "Jane", "Smith"),
          users += User(3L, "Bob", "Johnson")
        )
        .transactionally

      println("Inserting sample data...")
      val insertFuture = db.run(insertOps)
      Await.result(insertFuture, 5.seconds)
      println("Sample data inserted successfully")

      // Query with proper logging
      println("Querying for users with last name 'Doe'...")
      val queryResults = Await.result(
        db.run(users.filter(_.lastName === "Doe").result),
        5.seconds
      )
      println(s"Found ${queryResults.length} results:")
      queryResults.foreach(println)

    } catch {
      case e: Exception =>
        println(s"Error with Spanner operation: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("Closing database connection")
      db.close()
    }
  }
}
