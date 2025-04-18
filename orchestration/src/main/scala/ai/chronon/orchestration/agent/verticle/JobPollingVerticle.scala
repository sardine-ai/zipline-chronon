package ai.chronon.orchestration.agent.verticle

import ai.chronon.orchestration.agent.{AgentConfig, JobExecutionService, KVStore}
import ai.chronon.orchestration.agent.handlers.JobPollingHandler
import io.vertx.core.{AbstractVerticle, Promise}
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicBoolean

/** Verticle responsible for polling the orchestration service for job requests
  * and submitting them to the job executor service.
  *
  * This verticle:
  * 1. Periodically polls the orchestration service for jobs
  * 2. Stores job information in a KV store
  * 3. Submits jobs to the cluster for execution
  */
class JobPollingVerticle extends AbstractVerticle {
  private val logger: Logger = LoggerFactory.getLogger(classOf[JobPollingVerticle])

  private var webClient: WebClient = _
  private var kvStore: KVStore = _
  private var jobExecutionService: JobExecutionService = _
  private var jobPollingHandler: JobPollingHandler = _

  private val isPolling = new AtomicBoolean(false)
  private var pollingTimerId: Long = -1

  override def start(startPromise: Promise[Void]): Unit = {
    try {
      logger.info("Starting JobPollingVerticle")

      // Initialize dependencies
      initDependencies()

      // Start polling
      startPolling(startPromise)
    } catch {
      case e: Exception =>
        logger.error("Failed to start JobPollingVerticle", e)
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

    // Initialize polling handler
    jobPollingHandler = new JobPollingHandler(
      webClient,
      kvStore,
      jobExecutionService
    )

    logger.info(s"Initialized JobPollingVerticle with pollingIntervalMs=${AgentConfig.pollingIntervalMs}")
  }

  private def createKVStore(): KVStore = {
    KVStore.createInMemory()
  }

  private def createJobExecutionService(): JobExecutionService = {
    JobExecutionService.createInMemory()
  }

  private def startPolling(startPromise: Promise[Void]): Unit = {
    if (isPolling.compareAndSet(false, true)) {
      logger.info("Starting job polling")

      // Set up periodic polling with handler
      pollingTimerId = vertx.setPeriodic(AgentConfig.pollingIntervalMs, jobPollingHandler)

      startPromise.complete()
    } else {
      startPromise.complete()
    }
  }

  override def stop(stopPromise: Promise[Void]): Unit = {
    logger.info("Stopping JobPollingVerticle")
    isPolling.set(false)

    // Cancel the periodic timer if active
    if (pollingTimerId != -1) {
      vertx.cancelTimer(pollingTimerId)
    }

    // Close web client
    if (webClient != null) {
      webClient.close()
    }

    stopPromise.complete()
  }
}

object JobPollingVerticle {

  /** Creates a new JobPollingVerticle with custom dependencies.
    * This method is primarily used for testing to inject mock dependencies.
    */
  def createWithDependencies(
      kvStore: KVStore,
      jobExecutionService: JobExecutionService
  ): JobPollingVerticle = {
    val verticle = new JobPollingVerticle()
    verticle.kvStore = kvStore
    verticle.jobExecutionService = jobExecutionService
    verticle
  }
}
