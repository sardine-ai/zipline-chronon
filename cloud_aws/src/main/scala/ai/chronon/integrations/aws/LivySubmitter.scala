package ai.chronon.integrations.aws

import ai.chronon.spark.{JobSubmitter, JobType}

class LivySubmitter extends JobSubmitter {

  override def submit(jobType: JobType,
                      jobProperties: Map[String, String],
                      files: List[String],
                      args: String*): String = ???

  override def status(jobId: String): Unit = ???

  override def kill(jobId: String): Unit = ???
}
