package ai.chronon.orchestration.agent

import ai.chronon.api.{Job, JobStatusType}

/** TODO: To extend this further if needed and have implementations for both GCP and AWS.
  * Just a place holder will change a lot in coming PRs
  */
trait JobExecutionService {

  def submitJob(job: Job): Unit

  def getJobStatus(jobId: String): JobStatusType
}

/** In-memory implementation of JobExecutionService for testing and development.
  */
class InMemoryJobExecutionService extends JobExecutionService {
  private val jobStatuses = scala.collection.mutable.Map[String, JobStatusType]()

  override def submitJob(job: Job): Unit = {
    jobStatuses.put(job.jobInfo.getJobId, JobStatusType.RUNNING)
  }

  override def getJobStatus(jobId: String): JobStatusType = {
    jobStatuses.getOrElse(jobId, JobStatusType.UNKNOWN)
  }
}

object JobExecutionService {
  def createInMemory(): JobExecutionService = new InMemoryJobExecutionService()
}
