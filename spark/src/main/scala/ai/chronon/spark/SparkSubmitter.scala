package ai.chronon.spark

trait SparkSubmitter {

  def submit(files: List[String], args: String*): String

  def status(jobId: String): Unit

  def kill(jobId: String): Unit
}

abstract class SparkAuth {
  def token(): Unit = {}
}
