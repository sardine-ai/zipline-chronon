package ai.chronon.orchestration.agent.handlers

import ai.chronon.agent.JobStore
import ai.chronon.api.JobStatusType
import ai.chronon.orchestration.agent.{AgentConfig, JobExecutionService}
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.{HttpResponse, WebClient}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Handler for status reporting operations. */
class StatusReportingHandler(
    webClient: WebClient,
    jobStore: JobStore,
    jobExecutionService: JobExecutionService
) extends Handler[java.lang.Long] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StatusReportingHandler])

  /** Vert.x Handler implementation that gets called periodically.
    *
    * @param timerId The ID of the timer that triggered this handler
    */
  override def handle(timerId: java.lang.Long): Unit = {
    reportJobStatuses()
  }

  /** Checks the status of all jobs and reports changes. */
  private def reportJobStatuses(): Unit = {
    logger.debug("Checking for job status updates")

    try {
      // Get all active jobs from job store
      val jobs = jobStore.getAllActiveJobs

      // For each job, check current status and report if changed
      jobs.foreach { job =>
        val jobId = job.jobInfo.getJobId
        val currentStatus = job.jobInfo.getCurrentStatus
        val latestStatus = jobExecutionService.getJobStatus(jobId)

        if (currentStatus != latestStatus) {
          logger.info(s"Job $jobId status changed from $currentStatus to $latestStatus")

          // Update status in job store
          jobStore.updateJobStatus(jobId, latestStatus)

          // Report status change to orchestration service
          reportStatusToOrchestration(jobId, latestStatus)
        }
      }
    } catch {
      case e: Exception =>
        logger.error("Error while reporting job statuses", e)
    }
  }

  /** Reports a job status update to the orchestration service. */
  private def reportStatusToOrchestration(jobId: String, status: JobStatusType): Unit = {
    // Create status update payload
    val statusUpdate = new JsonObject()
      .put("jobId", jobId)
      .put("status", status.toString)
      .put("timestamp", System.currentTimeMillis())

    val host = AgentConfig.orchestrationServiceHostname
    val port = AgentConfig.orchestrationServicePort

    logger.info(s"Connecting to host:$host at port:$port")

    // Send status update to orchestration service
    webClient
      .put(port, host, s"/jobs/$jobId/status")
      .sendJsonObject(statusUpdate, createStatusUpdateHandler(jobId, status))
  }

  /** Creates a response handler for status update HTTP requests */
  private def createStatusUpdateHandler(
      jobId: String,
      status: JobStatusType): Handler[AsyncResult[HttpResponse[Buffer]]] = { (ar: AsyncResult[HttpResponse[Buffer]]) =>
    {
      if (ar.succeeded()) {
        val response = ar.result()
        if (response.statusCode() == 200) {
          logger.info(s"Successfully reported status update for job $jobId: $status")
        } else {
          logger.error(s"Failed to report status update: ${response.statusCode()} ${response.statusMessage()}")
        }
      } else {
        logger.error("Failed to report status update", ar.cause())
      }
    }
  }
}
