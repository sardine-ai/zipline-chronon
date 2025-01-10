package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.JobAuth
import ai.chronon.spark.JobSubmitter
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1.LoggingConfig.Level
import com.google.cloud.dataproc.v1._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.yaml.snakeyaml.Yaml

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

class DataprocSubmitter(jobControllerClient: JobControllerClient, conf: SubmitterConf) extends JobSubmitter {

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
      .setLoggingConfig(
        LoggingConfig
          .newBuilder()
          .putDriverLogLevels("ai.chronon.spark", Level.INFO)
          .build())
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

  def apply(conf: SubmitterConf): DataprocSubmitter = {
    val jobControllerClient = JobControllerClient.create(
      JobControllerSettings.newBuilder().setEndpoint(conf.endPoint).build()
    )
    new DataprocSubmitter(jobControllerClient, conf)
  }

  private[cloud_gcp] def loadConfig: SubmitterConf = {
    val inputStreamOption = Option(getClass.getClassLoader.getResourceAsStream("dataproc-submitter-conf.yaml"))
    val yamlLoader = new Yaml()
    implicit val formats: Formats = DefaultFormats
    inputStreamOption
      .map(Source.fromInputStream)
      .map((is) =>
        try { is.mkString }
        finally { is.close })
      .map(yamlLoader.load(_).asInstanceOf[java.util.Map[String, Any]])
      .map((jMap) => Extraction.decompose(jMap.asScala.toMap))
      .map((jVal) => render(jVal))
      .map(compact)
      .map(parse(_).extract[SubmitterConf])
      .getOrElse(throw new IllegalArgumentException("Yaml conf not found or invalid yaml"))

  }
  def main(args: Array[String]): Unit = {
    val chrononJarUri = args.filter(_.startsWith("--chronon_jar_uri"))(0).split("=")(1)

    // search args array for prefix `--gcs_files`
    val gcsFiles = args
      .filter(_.startsWith("--gcs_files"))(0)
      .split("=")(1)
      .split(",")

    val userArgs = args.filter(f => !f.startsWith("--gcs_files") && !f.startsWith("--chronon_jar_uri"))

    val required_vars = List.apply(
      "ZIPLINE_GCP_PROJECT_ID",
      "ZIPLINE_GCP_REGION",
      "ZIPLINE_GCP_DATAPROC_CLUSTER_NAME"
    )
    val missing_vars = required_vars.filter(!sys.env.contains(_))
    if (missing_vars.nonEmpty) {
      throw new Exception(s"Missing required environment variables: ${missing_vars.mkString(", ")}")
    }

    val projectId = sys.env.getOrElse("ZIPLINE_GCP_PROJECT_ID", throw new Exception("ZIPLINE_GCP_PROJECT_ID not set"))
    val region = sys.env.getOrElse("ZIPLINE_GCP_REGION", throw new Exception("ZIPLINE_GCP_REGION not set"))
    val clusterName = sys.env
      .getOrElse("ZIPLINE_GCP_DATAPROC_CLUSTER_NAME", throw new Exception("ZIPLINE_GCP_DATAPROC_CLUSTER_NAME not set"))

    val submitterConf = SubmitterConf(
      projectId,
      region,
      clusterName,
      chrononJarUri,
      "ai.chronon.spark.Driver"
    )

    val a = DataprocSubmitter(submitterConf)

    val jobId = a.submit(
      gcsFiles.toList,
      userArgs: _*
    )
    println("Dataproc submitter job id: " + jobId)
  }
}

object DataprocAuth extends JobAuth {}
