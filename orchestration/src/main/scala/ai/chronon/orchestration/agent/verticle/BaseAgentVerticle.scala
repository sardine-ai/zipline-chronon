package ai.chronon.orchestration.agent.verticle

import ai.chronon.agent.cloud_gcp.{BigTableKVJobStore, GcpJobExecutor}
import ai.chronon.agent.{JobExecutor, JobStore}
import ai.chronon.orchestration.agent.AgentConfig
import io.vertx.core.{AbstractVerticle, Handler, Promise}
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicBoolean

/** Base abstract class for agent verticles that provides common functionality.
  * Concrete verticles only need to implement:
  * - getVerticleName: String - for logging
  * - createHandler(): Handler[java.lang.Long] - to create the appropriate handler
  * - getIntervalMs: Long - to get the appropriate interval
  */
abstract class BaseAgentVerticle extends AbstractVerticle {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  protected var webClient: WebClient = _
  protected var jobStore: JobStore = _
  protected var jobExecutor: JobExecutor = _

  private val isActive = new AtomicBoolean(false)
  private var timerId: Long = -1

  protected def getVerticleName: String
  protected def createHandler(): Handler[java.lang.Long]
  protected def getIntervalMs: Long

  override def start(startPromise: Promise[Void]): Unit = {
    try {
      logger.info(s"Starting $getVerticleName")

      // Initialize dependencies
      initDependencies()

      // Start periodic task
      startPeriodicTask(startPromise)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to start $getVerticleName", e)
        startPromise.fail(e)
    }
  }

  protected def initDependencies(): Unit = {
    // Create web client for HTTP requests
    webClient = WebClient.create(vertx)

    // Initialize job store if not already set
    if (jobStore == null) {
      jobStore = createJobStore()
    }

    // Initialize job executor if not already set
    if (jobExecutor == null) {
      jobExecutor = createJobExecutor()
    }

    logger.info(s"Initialized $getVerticleName with intervalMs=$getIntervalMs")
  }

  private def createJobStore(): JobStore = {
    BigTableKVJobStore(AgentConfig.gcpProjectId, AgentConfig.bigTableInstanceId)
  }

  private def createJobExecutor(): JobExecutor = {
    new GcpJobExecutor(AgentConfig.gcpRegion, AgentConfig.gcpProjectId, AgentConfig.gcpCustomerId)
  }

  private def startPeriodicTask(startPromise: Promise[Void]): Unit = {
    if (isActive.compareAndSet(false, true)) {
      logger.info(s"Starting periodic task for $getVerticleName")

      // Set up periodic task with handler
      timerId = vertx.setPeriodic(getIntervalMs, createHandler())

      startPromise.complete()
    } else {
      startPromise.complete()
    }
  }

  override def stop(stopPromise: Promise[Void]): Unit = {
    logger.info(s"Stopping $getVerticleName")
    isActive.set(false)

    // Cancel the periodic timer if active
    if (timerId != -1) {
      vertx.cancelTimer(timerId)
    }

    // Close resources
    if (webClient != null) {
      webClient.close()
    }

    stopPromise.complete()
  }
}
