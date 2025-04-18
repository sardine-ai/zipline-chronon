package ai.chronon.orchestration.agent.verticle

import ai.chronon.orchestration.agent.{AgentConfig, JobExecutionService, KVStore}
import ai.chronon.orchestration.agent.handlers.StatusReportingHandler
import io.vertx.core.{AbstractVerticle, Promise}
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicBoolean

/** Verticle responsible for reporting job status to the orchestration service.
  *
  * This verticle:
  * 1. Periodically polls the JobExecutionService for the status of active jobs
  * 2. Updates the KVStore with the latest status information
  * 3. Reports status changes to the orchestration service
  */
class StatusReportingVerticle extends AbstractVerticle {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StatusReportingVerticle])

  private var webClient: WebClient = _
  private var kvStore: KVStore = _
  private var jobExecutionService: JobExecutionService = _
  private var statusReportingHandler: StatusReportingHandler = _

  private val isReporting = new AtomicBoolean(false)
  private var reportingTimerId: Long = -1

  override def start(startPromise: Promise[Void]): Unit = {
    try {
      logger.info("Starting StatusReportingVerticle")

      // Initialize dependencies
      initDependencies()

      // Start status reporting
      startStatusReporting(startPromise)
    } catch {
      case e: Exception =>
        logger.error("Failed to start StatusReportingVerticle", e)
        startPromise.fail(e)
    }
  }

  private def initDependencies(): Unit = {
    // Create web client for HTTP requests
    webClient = WebClient.create(vertx)

    // Initialize KV store if not already set
    if (kvStore == null) {
      kvStore = createKVStore()
    }

    // Initialize job service if not already set
    if (jobExecutionService == null) {
      jobExecutionService = createJobExecutionService()
    }

    // Initialize status reporting handler
    statusReportingHandler = new StatusReportingHandler(
      webClient,
      kvStore,
      jobExecutionService
    )

    logger.info(
      s"Initialized StatusReportingVerticle with reportingIntervalMs=${AgentConfig.statusReportingIntervalMs}")
  }

  private def createKVStore(): KVStore = {
    // For now, use the in-memory implementation
    // In production, this would be replaced with a real implementation
    KVStore.createInMemory()
  }

  private def createJobExecutionService(): JobExecutionService = {
    // For now, use the in-memory implementation
    // In production, this would be replaced with a real implementation
    JobExecutionService.createInMemory()
  }

  private def startStatusReporting(startPromise: Promise[Void]): Unit = {
    if (isReporting.compareAndSet(false, true)) {
      logger.info("Starting status reporting")

      // Set up periodic status reporting with handler
      reportingTimerId = vertx.setPeriodic(AgentConfig.statusReportingIntervalMs, statusReportingHandler)

      startPromise.complete()
    } else {
      startPromise.complete()
    }
  }

  override def stop(stopPromise: Promise[Void]): Unit = {
    logger.info("Stopping StatusReportingVerticle")
    isReporting.set(false)

    // Cancel the periodic timer if active
    if (reportingTimerId != -1) {
      vertx.cancelTimer(reportingTimerId)
    }

    // Close web client
    if (webClient != null) {
      webClient.close()
    }

    stopPromise.complete()
  }
}

object StatusReportingVerticle {

  /** Creates a new StatusReportingVerticle with custom dependencies.
    * This method is primarily used for testing to inject mock dependencies.
    */
  def createWithDependencies(
      kvStore: KVStore,
      jobExecutionService: JobExecutionService
  ): StatusReportingVerticle = {
    val verticle = new StatusReportingVerticle()
    verticle.kvStore = kvStore
    verticle.jobExecutionService = jobExecutionService
    verticle
  }
}
