package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.SparkAuth
import ai.chronon.spark.SparkSubmitter
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source

import collection.JavaConverters._

case class SubmitterConf(
    projectId: String,
    region: String,
    clusterName: String,
    jarUri: String,
    mainClass: String
) {

  def endPoint: String = s"${region}-dataproc.googleapis.com:443"
}

case class GeneralJob(
    jobName: String,
    jars: String,
    mainClass: String
)

class DataprocSubmitter(jobControllerClient: JobControllerClient, conf: SubmitterConf) extends SparkSubmitter {

  override def status(jobId: String): Unit = {
    try {
      val currentJob: Job = jobControllerClient.getJob(conf.projectId, conf.region, jobId)
      currentJob.getStatus.getState
    } catch {
      case e: ApiException =>
        println(s"Error monitoring job: ${e.getMessage}")
    }
  }

  override def kill(jobId: String): Unit = {
    val job = jobControllerClient.cancelJob(conf.projectId, conf.region, jobId)
    job.getDone
  }

  override def submit(files: List[String], args: String*): String = {
    val sparkJob = SparkJob
      .newBuilder()
      .setMainClass(conf.mainClass)
      .addJarFileUris(conf.jarUri)
      .addAllFileUris(files.asJava)
      .addAllArgs(args.toIterable.asJava)
      .build()

    val jobPlacement = JobPlacement
      .newBuilder()
      .setClusterName(conf.clusterName)
      .build()

    try {
      val job = Job
        .newBuilder()
        .setReference(jobReference)
        .setPlacement(jobPlacement)
        .setSparkJob(sparkJob)
        .build()

      val submittedJob = jobControllerClient.submitJob(conf.projectId, conf.region, job)
      submittedJob.getReference.getJobId

    } catch {
      case e: ApiException =>
        throw new RuntimeException(s"Failed to submit job: ${e.getMessage}")
    }
  }

  def jobReference: JobReference = JobReference.newBuilder().build()
}

object DataprocSubmitter {
  def apply(): DataprocSubmitter = {
    val conf = loadConfig
    val jobControllerClient = JobControllerClient.create(
      JobControllerSettings.newBuilder().setEndpoint(conf.endPoint).build()
    )
    new DataprocSubmitter(jobControllerClient, conf)
  }

  def loadConfig: SubmitterConf = {
    val is = getClass.getClassLoader.getResourceAsStream("dataproc-submitter-conf.yaml")
    val confStr = Source.fromInputStream(is).mkString
    val res: Either[io.circe.Error, SubmitterConf] = parser
      .parse(confStr)
      .flatMap(_.as[SubmitterConf])
    res match {

      case Right(v) => v
      case Left(e)  => throw e
    }
  }
}

object DataprocAuth extends SparkAuth {}
