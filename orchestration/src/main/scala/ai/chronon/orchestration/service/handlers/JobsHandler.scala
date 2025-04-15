package ai.chronon.orchestration.service.handlers

import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Job, JobListGetRequest, JobListResponse}
import ai.chronon.orchestration.pubsub.PubSubManager
import org.slf4j.{Logger, LoggerFactory}

/** Handler for job management operations.
  *
  * @param pubSubManager The PubSub manager used to create subscribers
  */
class JobsHandler(pubSubManager: PubSubManager) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Create a unique subscription ID based on service name
  // Multiple instances of the service should share subscription ID to distribute the message processing load.
  private val subscriptionId = "orchestration-service"

  /** Gets a list of jobs that need to be executed.
    *
    * This method pulls messages from the PubSub subscription and converts them
    * to Job objects that can be executed by the agent.
    *
    * @return a response containing jobs to start and stop
    */
  def getJobs(jobListGetRequest: JobListGetRequest): JobListResponse = {
    // TODO: Authenticate that the token in the request is valid for pulling the job requests
    val subscriber = pubSubManager.getOrCreateSubscriber(jobListGetRequest.topicId, subscriptionId)

    // Pull messages from the subscription
    val messages = subscriber.pullMessages()

    // Convert messages to Job objects
    val jobs = messages.flatMap { message =>
      // Extract node name attribute which is required
      val nodeName = message.getAttributes.getOrElse("nodeName", "")
      if (nodeName.isEmpty) {
        logger.warn(s"Skipping message without nodeName attribute")
        None
      } else {
        // Create a unique job ID based on the node name and a timestamp
        val jobId = s"$nodeName-${System.currentTimeMillis()}"

        // Convert message attributes to a Job instance
        val job = new Job()
          .setJobId(jobId)
        // TODO: Add more job properties as needed

        Some(job)
      }
    }

    // Create the response with all available jobs
    new JobListResponse()
      .setJobsToStart(jobs.toJava)
      // TODO: For now, we don't have any jobs to stop
      .setJobsToStop(new java.util.ArrayList[String]())
  }
}
