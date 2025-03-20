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

  // Set up PostgresSQL test container
  private val postgresContainer = new PostgreSQLContainer("postgres:14")

  // Implement database connection using PostgresSQL TestContainers
  protected lazy val db: Database = {
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
    db.close()
    super.afterAll()
    postgresContainer.stop()
  }
}
