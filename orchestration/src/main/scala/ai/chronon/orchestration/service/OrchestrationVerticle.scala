package ai.chronon.orchestration.service

import ai.chronon.api.JobListGetRequest
import ai.chronon.orchestration.{DiffRequest, UploadRequest}
import ai.chronon.orchestration.persistence.ConfDao
import ai.chronon.orchestration.pubsub.PubSubManager
import ai.chronon.orchestration.service.handlers.{JobsHandler, UploadHandler}
import ai.chronon.service.{ConfigStore, RouteHandlerWrapper}
import io.vertx.core.http.{HttpMethod, HttpServer, HttpServerOptions}
import io.vertx.core.{AbstractVerticle, Promise, Vertx}
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.{BodyHandler, CorsHandler}
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database

/** Verticle for the Orchestration Service.
  *
  * This is the main entry point for the Orchestration Service. It sets up routes
  * for various operations like uploading data, retrieving jobs, etc. It also
  * handles CORS configuration and server lifecycle.
  *
  * The Verticle has been designed to be more testable by using dependency injection:
  * 1. Dependencies can be provided through the factory companion object
  * 2. The main start method delegates to initDependencies for dependency creation
  */
class OrchestrationVerticle extends AbstractVerticle {
  private val logger = LoggerFactory.getLogger(classOf[OrchestrationVerticle])
  private var db: Database = _
  private var server: HttpServer = _
  private var pubSubManager: PubSubManager = _

  /** Main entry point for starting the verticle.
    * This delegates to initDependencies to create dependencies if they don't exist.
    */
  override def start(startPromise: Promise[Void]): Unit = {
    val cfgStore = new ConfigStore(vertx)

    // Initialize dependencies if they haven't been set
    initDependencies(cfgStore)

    startHttpServer(
      cfgStore.getServerPort,
      cfgStore.encodeConfig(),
      startPromise
    )
  }

  /** Initializes dependencies if they haven't been set.
    * This method allows for clean separation between dependency creation and usage.
    */
  private def initDependencies(cfgStore: ConfigStore): Unit = {
    // Initialize PubSub if not already set
    if (pubSubManager == null) {
      pubSubManager = createPubSubManager(cfgStore)
    }

    // Initialize DB if not already set
    if (db == null) {
      db = createDatabase(cfgStore)
    }
  }

  /** Creates a database connection.
    */
  private def createDatabase(cfgStore: ConfigStore): Database = {
    // Validate database configuration
    cfgStore.validateDatabaseConfig()

    // Get database configuration from ConfigStore
    val jdbcUrl = cfgStore.getJdbcUrl
    val username = cfgStore.getJdbcUsername
    val password = cfgStore.getJdbcPassword

    logger.info(s"Connecting to database at $jdbcUrl")
    Database.forURL(jdbcUrl, username, password)
  }

  /** Creates a PubSubManager.
    */
  private def createPubSubManager(cfgStore: ConfigStore): PubSubManager = {
    // Validate GCP configuration
    cfgStore.validateGcpConfig()

    // Get GCP project ID from ConfigStore
    val projectId = cfgStore.getGcpProjectId

    // Check if we should use the emulator (from environment variable)
    val useEmulator = System.getenv().getOrDefault("USE_PUBSUB_EMULATOR", "false").toBoolean

    if (useEmulator) {
      val emulatorHost = System.getenv().getOrDefault("PUBSUB_EMULATOR_HOST", "localhost:8085")
      logger.info(s"Using PubSub emulator for project $projectId with host $emulatorHost")
      PubSubManager.forEmulator(projectId, emulatorHost)
    } else {
      logger.info(s"Using production PubSub for project $projectId")
      PubSubManager.forProduction(projectId)
    }
  }

  /** Starts the HTTP server with configured routes.
    *
    * @param port the port to listen on
    * @param configJsonString the configuration as a JSON string
    * @param startPromise the promise to complete when initialization is done
    */
  @throws[Exception]
  protected def startHttpServer(port: Int, configJsonString: String, startPromise: Promise[Void]): Unit = {
    // Create and configure the router
    val router = setupRouter(configJsonString)

    // Start HTTP server
    val httpOptions = new HttpServerOptions()
      .setTcpKeepAlive(true)
      .setIdleTimeout(60)
    server = vertx.createHttpServer(httpOptions)
    server
      .requestHandler(router)
      .listen(port)
      .onSuccess(serverInstance => {
        logger.info("HTTP server started on port {}", serverInstance.actualPort())
        startPromise.complete()
      })
      .onFailure(err => {
        logger.error("Failed to start HTTP server", err)
        startPromise.fail(err)
      })
  }

  /** Sets up the router with all routes.
    * This method is extracted for testability - tests can call this
    * to get a configured router without starting an actual HTTP server.
    *
    * @param configJsonString the configuration as a JSON string
    * @return a configured router with all routes set up
    */
  def setupRouter(configJsonString: String): Router = {
    val router = Router.router(vertx)

    // Apply global middleware
    router.route().handler(BodyHandler.create())
    wireUpCORSConfig(router)

    // Set up routes
    setupHealthRoutes(router, configJsonString)
    setupUploadRoutes(router)
    setupJobRoutes(router)

    router
  }

  /** Sets up health check and configuration routes.
    *
    * @param router the router to add routes to
    * @param configJsonString the configuration as a JSON string
    */
  private def setupHealthRoutes(router: Router, configJsonString: String): Unit = {
    // Health check route
    router
      .get("/ping")
      .handler(ctx => {
        ctx.json("Pong!")
      })

    // Route to show current configuration
    router
      .get("/config")
      .handler(ctx => {
        ctx
          .response()
          .putHeader("content-type", "application/json")
          .end(configJsonString)
      })
  }

  /** Sets up upload-related routes.
    *
    * @param router the router to add routes to
    */
  private def setupUploadRoutes(router: Router): Unit = {
    val dao = new ConfDao(this.db)
    val uploadHandler = new UploadHandler(dao)

    router
      .get("/upload/v1/diff")
      .handler(RouteHandlerWrapper.createHandler(uploadHandler.getDiff, classOf[DiffRequest]))

    router
      .post("/upload/v1/confs")
      .handler(RouteHandlerWrapper.createHandler(uploadHandler.upload, classOf[UploadRequest]))
  }

  /** Sets up job-related routes.
    *
    * @param router the router to add routes to
    */
  private def setupJobRoutes(router: Router): Unit = {
    val jobsHandler = new JobsHandler(pubSubManager)

    // Add route for getting available jobs
    router
      .get("/jobs/list")
      .handler(RouteHandlerWrapper.createHandler(jobsHandler.getJobs, classOf[JobListGetRequest]))
  }

  override def stop(stopPromise: Promise[Void]): Unit = {
    logger.info("Stopping HTTP server...")
    if (server != null) {
      server
        .close()
        .onSuccess(_ => {
          logger.info("HTTP server stopped successfully")
          stopPromise.complete()
        })
        .onFailure(err => {
          logger.error("Failed to stop HTTP server", err)
          stopPromise.fail(err)
        })
    } else {
      stopPromise.complete()
    }
  }

  private def wireUpCORSConfig(router: Router): Unit = {
    router
      .route()
      .handler(
        CorsHandler
          .create()
          .addOrigin("http://localhost:5173")
          .addOrigin("http://localhost:3000")
          .allowedMethod(HttpMethod.GET)
          .allowedMethod(HttpMethod.POST)
          .allowedMethod(HttpMethod.PUT)
          .allowedMethod(HttpMethod.DELETE)
          .allowedMethod(HttpMethod.OPTIONS)
          .allowedHeader("Accept")
          .allowedHeader("Content-Type")
          .allowCredentials(false) // Change to true if credentials are required
      )
  }
}

object OrchestrationVerticle {

  /** Creates a new OrchestrationVerticle with custom dependencies.
    * This method is primarily used for testing to inject mock or test dependencies.
    *
    * @param vertx the Vertx instance
    * @param db the database instance
    * @param pubSubManager the PubSubManager instance
    * @return a new OrchestrationVerticle with the custom dependencies
    */
  def createWithDependencies(
      vertx: Vertx,
      db: Database,
      pubSubManager: PubSubManager
  ): OrchestrationVerticle = {
    val verticle = new OrchestrationVerticle()

    // Set the Vert.x instance
    verticle.init(vertx, null)

    // Set the dependencies directly
    verticle.db = db
    verticle.pubSubManager = pubSubManager

    verticle
  }
}
