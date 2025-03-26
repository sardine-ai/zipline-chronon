package ai.chronon.orchestration.service

import ai.chronon.orchestration.{DiffRequest, UploadRequest}
import ai.chronon.orchestration.persistence.ConfDao
import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpMethod
import org.slf4j.LoggerFactory
import io.vertx.ext.web.handler.CorsHandler
import ai.chronon.orchestration.service.handlers.UploadHandler
import ai.chronon.service.ConfigStore
import ai.chronon.service.RouteHandlerWrapper
import io.vertx.core.Promise
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import slick.jdbc.JdbcBackend.Database

class OrchestrationVerticle extends AbstractVerticle {
  private var db: Database = _

  private var server: HttpServer = _

  override def start(startPromise: Promise[Void]): Unit = {
    val cfgStore = new ConfigStore(vertx)
    startHttpServer(
      cfgStore.getServerPort(),
      cfgStore.encodeConfig(),
      startPromise
    )
  }

  def startAndSetDb(startPromise: Promise[Void], db: Database): Unit = {
    this.db = db
    start(startPromise)
  }

  @throws[Exception]
  protected def startHttpServer(port: Int, configJsonString: String, startPromise: Promise[Void]): Unit = {
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    val db = Database.forURL(
      url = "jdbc:postgresql://localhost:5432/postgres", // Default database name in PostgreSQL
      user = "postgres",                                 // Default PostgreSQL user
      password = "postgres",                             // Default password (change if different)
      driver = "org.postgresql.Driver"
    )
    this.db = db
    println("db: " + db)

    val router = Router.router(vertx)
    wireUpCORSConfig(router)
    
    // Important: Register BodyHandler BEFORE any routes that need request bodies
    router.route().handler(BodyHandler.create())

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

    // Routes for uploading data
    val dao = new ConfDao(this.db)
    val uploadHandler = new UploadHandler(dao)
    router
      .get("/upload/v1/diff")
      .handler(RouteHandlerWrapper.createHandler(uploadHandler.getDiff, classOf[DiffRequest]))
      
    router
      .post("/upload/v1/confs")
      .handler(RouteHandlerWrapper.createHandler(uploadHandler.upload, classOf[UploadRequest]))

    // Start HTTP server
    val httpOptions = new HttpServerOptions()
      .setTcpKeepAlive(true)
      .setIdleTimeout(60)
    server = vertx.createHttpServer(httpOptions)
    server
      .requestHandler(router)
      .listen(port)
      .onSuccess(serverInstance => {
        OrchestrationVerticle.logger.info("HTTP server started on port {}", serverInstance.actualPort())
        startPromise.complete()
      })
      .onFailure(err => {
        OrchestrationVerticle.logger.error("Failed to start HTTP server", err)
        startPromise.fail(err)
      })


  }

  override def stop(stopPromise: Promise[Void]): Unit = {
    OrchestrationVerticle.logger.info("Stopping HTTP server...")
    if (server != null) {
      server
        .close()
        .onSuccess(_ => {
          OrchestrationVerticle.logger.info("HTTP server stopped successfully")
          stopPromise.complete()
        })
        .onFailure(err => {
          OrchestrationVerticle.logger.error("Failed to stop HTTP server", err)
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
  private val logger = LoggerFactory.getLogger(classOf[OrchestrationVerticle])
}
