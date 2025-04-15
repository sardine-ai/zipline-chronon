package ai.chronon.orchestration.test.service

import ai.chronon.orchestration.pubsub.{PubSubManager, PubSubSubscriber}
import ai.chronon.orchestration.service.OrchestrationVerticle
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.ext.web.Route
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import slick.jdbc.JdbcBackend.Database

/** Tests for the OrchestrationVerticle class.
  *
  * This test focuses on verifying that the routes are set up correctly
  * and the dependencies are properly initialized.
  */
class OrchestrationVerticleSpec extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  private var vertx: Vertx = _
  private var mockDb: Database = _
  private var mockPubSubManager: PubSubManager = _
  private var mockSubscriber: PubSubSubscriber = _
  private var verticle: OrchestrationVerticle = _
  override def beforeAll(): Unit = {
    vertx = Vertx.vertx()
    mockDb = mock[Database]
    mockPubSubManager = mock[PubSubManager]
    mockSubscriber = mock[PubSubSubscriber]

    // Configure mocks
    when(mockPubSubManager.getOrCreateSubscriber(any[String], any[String]))
      .thenReturn(mockSubscriber)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    vertx.close()
    super.afterAll()
  }

  "OrchestrationVerticle" should "set up all required routes" in {
    // Create verticle with mock dependencies
    verticle = OrchestrationVerticle.createWithDependencies(vertx, mockDb, mockPubSubManager)

    // Set up the router without starting the server
    val router = verticle.setupRouter("{}")

    // Get all routes
    val routes = scala.collection.mutable.ListBuffer[Route]()
    router.getRoutes.forEach(route => routes += route)

    // Verify health routes exist
    routes.exists(route => route.getPath == "/ping") shouldBe true
    routes.exists(route => route.getPath == "/config") shouldBe true

    // Verify upload routes exist
    routes.exists(route => route.getPath == "/upload/v1/diff") shouldBe true
    routes.exists(route => route.getPath == "/upload/v1/confs") shouldBe true

    // Verify jobs routes exist with GET method
    val jobsRoute = routes.find(route => route.getPath == "/jobs/list")
    jobsRoute.isDefined shouldBe true
    // Verify the jobsRoute uses GET method
    jobsRoute.get.methods().contains(HttpMethod.GET) shouldBe true
  }
}
