package ai.chronon.orchestration.agent.verticle

import ai.chronon.agent.{JobExecutor, JobStore}
import ai.chronon.orchestration.agent.AgentConfig
import ai.chronon.orchestration.agent.handlers.JobPollingHandler
import io.vertx.core.Handler

/** Verticle responsible for polling the orchestration service for job requests
  * and submitting them to the job executor service.
  *
  * This verticle:
  * 1. Periodically polls the orchestration service for jobs
  * 2. Stores job information in a job store
  * 3. Submits jobs to the cluster for execution
  */
class JobPollingVerticle extends BaseAgentVerticle {

  private var jobPollingHandler: JobPollingHandler = _

  override protected def getVerticleName: String = "JobPollingVerticle"

  override protected def getIntervalMs: Long = AgentConfig.pollingIntervalMs

  override protected def initDependencies(): Unit = {
    super.initDependencies()

    // Initialize polling handler
    jobPollingHandler = new JobPollingHandler(
      webClient,
      jobStore,
      jobExecutor
    )
  }

  override protected def createHandler(): Handler[java.lang.Long] = jobPollingHandler
}

object JobPollingVerticle {

  /** Creates a new JobPollingVerticle with custom dependencies.
    * This method is primarily used for testing to inject mock dependencies.
    */
  def createWithDependencies(
      jobStore: JobStore,
      jobExecutor: JobExecutor
  ): JobPollingVerticle = {
    val verticle = new JobPollingVerticle()
    verticle.jobStore = jobStore
    verticle.jobExecutor = jobExecutor
    verticle
  }
}
