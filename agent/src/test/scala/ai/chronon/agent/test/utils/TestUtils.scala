package agent.src.test.scala.ai.chronon.agent.test.utils

import ai.chronon.api.{Job, JobInfo, JobStatusType}

object TestUtils {

  def createTestJob(jobId: String): Job = {
    val jobInfo = new JobInfo()
    jobInfo.setJobId(jobId)
    jobInfo.setCurrentStatus(JobStatusType.PENDING)

    val job = new Job()
    job.setJobInfo(jobInfo)
    job
  }
}
