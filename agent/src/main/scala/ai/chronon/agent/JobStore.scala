package ai.chronon.agent

import ai.chronon.api.{Job, JobStatusType}

trait JobStore {
  def storeJob(jobId: String, job: Job): Unit
  def getJob(jobId: String): Option[Job]
  def getAllActiveJobs: List[Job]
  def updateJobStatus(jobId: String, status: JobStatusType): Unit
}
