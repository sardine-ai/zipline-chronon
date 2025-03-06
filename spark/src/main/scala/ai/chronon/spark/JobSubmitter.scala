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
  val SavepointUri = "savepointUri"
  val FlinkStateUri = "flinkStateUri"

  // EMR specific properties
  val ClusterInstanceCount = "clusterInstanceCount"
  val ClusterInstanceType = "clusterInstanceType"
  val ClusterIdleTimeout = "clusterIdleTimeout"
  val EmrReleaseLabel = "emrReleaseLabel"
  val ShouldCreateCluster = "shouldCreateCluster"
  val JobFlowId = "jobFlowId"

  val JarUriArgKeyword = "--jar-uri"
  val JobTypeArgKeyword = "--job-type"
  val MainClassKeyword = "--main-class"
  val FlinkMainJarUriArgKeyword = "--flink-main-jar-uri"
  val FlinkSavepointUriArgKeyword = "--savepoint-uri"

}
