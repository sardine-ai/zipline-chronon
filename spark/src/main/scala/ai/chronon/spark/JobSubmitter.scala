package ai.chronon.spark

trait JobSubmitter {

  def submit(files: List[String], args: String*): String

  def status(jobId: String): Unit

  def kill(jobId: String): Unit
}

abstract class JobAuth {
  def token(): Unit = {}
}
