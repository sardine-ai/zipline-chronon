package ai.chronon.orchestration.agent.handlers

import ai.chronon.agent.{JobExecutor, JobStore}
import ai.chronon.api.{Job, JobInfo, JobStatusType}
import ai.chronon.orchestration.agent.AgentConfig
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.{HttpResponse, WebClient}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/** Handler for job polling operations. */
class JobPollingHandler(
    webClient: WebClient,
    jobStore: JobStore,
    jobExecutor: JobExecutor
) extends Handler[java.lang.Long] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[JobPollingHandler])

  /** Vert.x Handler implementation that gets called periodically.
    *
    * @param timerId The ID of the timer that triggered this handler
    */
  override def handle(timerId: java.lang.Long): Unit = {
    pollForJobs()
  }

  /** Polls the orchestration service for new jobs. */
  private def pollForJobs(): Unit = {
    logger.info(s"Polling for jobs with topicId: ${AgentConfig.topicId}")

    try {
      val host = AgentConfig.orchestrationServiceHostname
      val port = AgentConfig.orchestrationServicePort

      logger.info(s"Connecting to host:$host at port:$port")

      // Make HTTP request to orchestration service
      webClient
        .get(port, host, "/jobs/list")
        .addQueryParam("topic_id", AgentConfig.topicId)
        .send(createJobPollingResponseHandler())
    } catch {
      case e: Exception =>
        logger.error("Error while polling for jobs", e)
    }
  }

  /** Creates a response handler for job polling HTTP requests */
  private def createJobPollingResponseHandler(): Handler[AsyncResult[HttpResponse[Buffer]]] = {
    (ar: AsyncResult[HttpResponse[Buffer]]) =>
      {
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val responseBody = response.bodyAsJsonObject()
            processJobsResponse(responseBody)
          } else {
            logger.error(s"Failed to poll for jobs: ${response.statusCode()} ${response.statusMessage()}")
          }
        } else {
          logger.error("Failed to poll for jobs", ar.cause())
        }
      }
  }

  /** Processes the response from the orchestration service containing jobs. */
  private def processJobsResponse(responseJson: JsonObject): Unit = {
    Try {
      // Extract jobs to start
      val jobsToStartJson = responseJson.getJsonArray("jobsToStart", new JsonArray())
      val jobsToStart = (0 until jobsToStartJson.size()).map { i =>
        val jobJson = jobsToStartJson.getJsonObject(i)

        // Create JobInfo
        val jobInfo = new JobInfo()
        jobInfo.setJobId(jobJson.getString("jobId"))
        // Set other job info properties as needed

        // Create Job with JobInfo
        val job = new Job()
        job.setJobInfo(jobInfo)
        // Set other job properties as needed

        job
      }

      logger.info(s"Received ${jobsToStart.size} jobs to start")

      // Process each job
      jobsToStart.foreach { job =>
        val storedJob = jobStore.getJob(job.jobInfo.getJobId)
        if (storedJob.isEmpty) {
          // Store the job in KVStore
          job.jobInfo.setCurrentStatus(JobStatusType.PENDING)
          jobStore.storeJob(job.jobInfo.getJobId, job)

          // Submit job to cluster
          jobExecutor.submitJob(job)
        } else if (storedJob.get.getJobInfo.currentStatus == JobStatusType.FAILED) {
          // Update the job status
          jobStore.updateJobStatus(job.jobInfo.getJobId, JobStatusType.PENDING)

          // Submit job to cluster
          jobExecutor.submitJob(job)
        }
      }
    } match {
      case Success(_) =>
        logger.info("Successfully processed jobs response")
      case Failure(e) =>
        logger.error("Error processing jobs response", e)
    }
  }
}
