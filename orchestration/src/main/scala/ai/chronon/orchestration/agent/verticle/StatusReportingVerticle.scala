package ai.chronon.orchestration.agent.verticle

import ai.chronon.agent.{JobExecutor, JobStore}
import ai.chronon.orchestration.agent.AgentConfig
import ai.chronon.orchestration.agent.handlers.StatusReportingHandler
import io.vertx.core.Handler

/** Verticle responsible for reporting job status to the orchestration service.
  *
  * This verticle:
  * 1. Periodically polls the JobExecutor for the status of active jobs
  * 2. Updates the JobStore with the latest status information
  * 3. Reports status changes to the orchestration service
  */
class StatusReportingVerticle extends BaseAgentVerticle {

  private var statusReportingHandler: StatusReportingHandler = _

  override protected def getVerticleName: String = "StatusReportingVerticle"
  
  override protected def getIntervalMs: Long = AgentConfig.statusReportingIntervalMs
  
  override protected def initDependencies(): Unit = {
    super.initDependencies()
    
    // Initialize status reporting handler
    statusReportingHandler = new StatusReportingHandler(
      webClient,
      jobStore,
      jobExecutor
    )
  }
  
  override protected def createHandler(): Handler[java.lang.Long] = statusReportingHandler
}

object StatusReportingVerticle {
  /** Creates a new StatusReportingVerticle with custom dependencies.
   * This method is primarily used for testing to inject mock dependencies.
   */
  def createWithDependencies(
      jobStore: JobStore,
      jobExecutor: JobExecutor
  ): StatusReportingVerticle = {
    val verticle = new StatusReportingVerticle()
    verticle.jobStore = jobStore
    verticle.jobExecutor = jobExecutor
    verticle
  }
}