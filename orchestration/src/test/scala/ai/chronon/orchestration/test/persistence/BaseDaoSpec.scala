package ai.chronon.orchestration.test.persistence

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.containers.PostgreSQLContainer
import slick.jdbc.JdbcBackend.Database
import slick.util.AsyncExecutor

import scala.concurrent.ExecutionContext

trait BaseDaoSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  // Configure patience for ScalaFutures
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  // Add an implicit execution context
  implicit val ec: ExecutionContext = ExecutionContext.global

  // To be implemented by concrete subclasses
  protected def db: Database

  // Method to clean up the database connection
  private def closeDatabaseConnection(): Unit = {
    db.close()
  }

  override def afterAll(): Unit = {
    closeDatabaseConnection()
    super.afterAll()
  }
}

trait PostgresContainerSpec extends BaseDaoSpec {
  // Set up PostgresSQL test container
  private val postgresContainer = new PostgreSQLContainer("postgres:14")

  // Implement database connection using TestContainers
  override protected lazy val db: Database = {
    Database.forURL(
      url = postgresContainer.getJdbcUrl,
      user = postgresContainer.getUsername,
      password = postgresContainer.getPassword,
      executor = AsyncExecutor("TestExecutor", numThreads = 5, queueSize = 100)
    )
  }

  override def beforeAll(): Unit = {
    postgresContainer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    postgresContainer.stop()
  }
}

trait PGAdapterIntegrationSpec extends BaseDaoSpec {
  // Ports for our services
  private val pgAdapterPort = 5432

  /** Implement database connection using PGAdapter
    * Currently PGAdapter needs to be running locally for the tests to work
    * TODO: To move start/stop of PGAdapter to the spec itself for easier testing
    * https://cloud.google.com/spanner/docs/pgadapter-emulator has instructions on connecting PGAdapter to the
    * Spanner emulator. Combined docker container is the easier option
    * Note: docker run command might not work as is on M4 mac, and we need to add the following option
    * `--platform linux/amd64`
    */
  override protected lazy val db: Database = Database.forURL(
    url = s"jdbc:postgresql://localhost:$pgAdapterPort/test-database",
    user = "",
    password = "",
    executor = AsyncExecutor("TestExecutor", numThreads = 5, queueSize = 100)
  )
}
