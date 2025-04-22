package ai.chronon.agent

import ai.chronon.api.{Job, JobStatusType}

trait JobExecutor {

  def submitJob(job: Job): Unit

  def getJobStatus(jobId: String): JobStatusType
}
