package ai.chronon.spark

sealed trait JobType
case object SparkJob extends JobType
case object FlinkJob extends JobType

trait JobSubmitter {

  def submit(jobType: JobType, jobProperties: Map[String, String], files: List[String], args: String*): String

  def status(jobId: String): Unit

  def kill(jobId: String): Unit
}

abstract class JobAuth {
  def token(): Unit = {}
}

object JobSubmitterConstants {
  val MainClass = "mainClass"
  val JarURI = "jarUri"
  val FlinkMainJarURI = "flinkMainJarUri"
}
