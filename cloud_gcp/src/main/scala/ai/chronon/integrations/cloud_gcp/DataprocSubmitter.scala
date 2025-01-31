package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.JobAuth
import ai.chronon.spark.JobSubmitter
import ai.chronon.spark.JobSubmitterConstants.FlinkMainJarURI
import ai.chronon.spark.JobSubmitterConstants.JarURI
import ai.chronon.spark.JobSubmitterConstants.MainClass
import ai.chronon.spark.JobSubmitterConstants.SavepointUri
import ai.chronon.spark.JobType
import ai.chronon.spark.{FlinkJob => TypeFlinkJob}
import ai.chronon.spark.{SparkJob => TypeSparkJob}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.yaml.snakeyaml.Yaml

import scala.io.Source

import collection.JavaConverters._

case class SubmitterConf(
    projectId: String,
    region: String,
    clusterName: String
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

  override def submit(jobType: JobType,
                      jobProperties: Map[String, String],
                      files: List[String],
                      args: String*): String = {
    val mainClass = jobProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
    val jarUri = jobProperties.getOrElse(JarURI, throw new RuntimeException("Jar URI not found"))

    val jobBuilder = jobType match {
      case TypeSparkJob => buildSparkJob(mainClass, jarUri, files, args: _*)
      case TypeFlinkJob =>
        val mainJarUri =
          jobProperties.getOrElse(FlinkMainJarURI, throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        val maybeSavepointUri = jobProperties.get(SavepointUri)
        buildFlinkJob(mainClass, mainJarUri, jarUri, maybeSavepointUri, args: _*)
    }

    val jobPlacement = JobPlacement
      .newBuilder()
      .setClusterName(conf.clusterName)
      .build()

    try {
      val job = jobBuilder
        .setReference(jobReference)
        .setPlacement(jobPlacement)
        .build()

      val submittedJob = jobControllerClient.submitJob(conf.projectId, conf.region, job)
      submittedJob.getReference.getJobId

    } catch {
      case e: ApiException =>
        throw new RuntimeException(s"Failed to submit job: ${e.getMessage}", e)
    }
  }

  private def buildSparkJob(mainClass: String, jarUri: String, files: List[String], args: String*): Job.Builder = {
    val sparkJob = SparkJob
      .newBuilder()
      .setMainClass(mainClass)
      .addJarFileUris(jarUri)
      .addAllFileUris(files.asJava)
      .addAllArgs(args.toIterable.asJava)
      .build()
    Job.newBuilder().setSparkJob(sparkJob)
  }

  private def buildFlinkJob(mainClass: String,
                            mainJarUri: String,
                            jarUri: String,
                            maybeSavePointUri: Option[String],
                            args: String*): Job.Builder = {

    // TODO leverage a setting in teams.json when that's wired up
    val checkpointsDir = "gs://zl-warehouse/flink-state"

    // JobManager is primarily responsible for coordinating the job (task slots, checkpoint triggering) and not much else
    // so 4G should suffice.
    // We go with 64G TM containers (4 task slots per container)
    // Broadly Flink splits TM memory into:
    // 1) Metaspace, framework offheap etc
    // 2) Network buffers
    // 3) Managed Memory (rocksdb)
    // 4) JVM heap
    // We tune down the network buffers to 1G-2G (default would be ~6.3G) and use some of the extra memory for
    // managed mem + jvm heap
    // Good doc - https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup_tm
    val envProps =
      Map(
        "jobmanager.memory.process.size" -> "4G",
        "taskmanager.memory.process.size" -> "64G",
        "taskmanager.memory.network.min" -> "1G",
        "taskmanager.memory.network.max" -> "2G",
        "taskmanager.memory.managed.fraction" -> "0.5f",
        "yarn.classpath.include-user-jar" -> "FIRST",
        "state.savepoints.dir" -> checkpointsDir,
        "state.checkpoints.dir" -> checkpointsDir,
        // override the local dir for rocksdb as the default ends up being too large file name size wise
        "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
        "state.checkpoint-storage" -> "filesystem"
      )

    val flinkJobBuilder = FlinkJob
      .newBuilder()
      .setMainClass(mainClass)
      .setMainJarFileUri(mainJarUri)
      .putAllProperties(envProps.asJava)
      .addJarFileUris(jarUri)
      .addAllArgs(args.toIterable.asJava)

    val updatedFlinkJobBuilder =
      maybeSavePointUri match {
        case Some(savePointUri) => flinkJobBuilder.setSavepointUri(savePointUri)
        case None               => flinkJobBuilder
      }

    Job.newBuilder().setFlinkJob(updatedFlinkJobBuilder.build())
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

  private val JAR_URI_ARG_PREFIX = "--jar-uri"
  private val GCS_FILES_ARG_PREFIX = "--gcs-files"
  private val JOB_TYPE_ARG_PREFIX = "--job-type"
  private val MAIN_CLASS_PREFIX = "--main-class"
  private val FLINK_MAIN_JAR_URI_ARG_PREFIX = "--flink-main-jar-uri"
  private val FLINK_SAVEPOINT_URI_ARG_PREFIX = "--savepoint-uri"

  def main(args: Array[String]): Unit = {

    // search args array for prefix `--gcs_files`
    val gcsFilesArgs = args.filter(_.startsWith(GCS_FILES_ARG_PREFIX))
    assert(gcsFilesArgs.length == 0 || gcsFilesArgs.length == 1)

    val gcsFiles = if (gcsFilesArgs.isEmpty) {
      Array.empty[String]
    } else {
      gcsFilesArgs(0).split("=")(1).split(",")
    }

    // Exclude args list that starts with `--gcs_files` or `--jar_uri`
    val internalArgs = Set(GCS_FILES_ARG_PREFIX,
                           JAR_URI_ARG_PREFIX,
                           JOB_TYPE_ARG_PREFIX,
                           MAIN_CLASS_PREFIX,
                           FLINK_MAIN_JAR_URI_ARG_PREFIX,
                           FLINK_SAVEPOINT_URI_ARG_PREFIX)
    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val required_vars = List.apply(
      "GCP_PROJECT_ID",
      "GCP_REGION",
      "GCP_DATAPROC_CLUSTER_NAME"
    )
    val missing_vars = required_vars.filter(!sys.env.contains(_))
    if (missing_vars.nonEmpty) {
      throw new Exception(s"Missing required environment variables: ${missing_vars.mkString(", ")}")
    }
    val projectId = sys.env.getOrElse("GCP_PROJECT_ID", throw new Exception("GCP_PROJECT_ID not set"))
    val region = sys.env.getOrElse("GCP_REGION", throw new Exception("GCP_REGION not set"))
    val clusterName = sys.env
      .getOrElse("GCP_DATAPROC_CLUSTER_NAME", throw new Exception("GCP_DATAPROC_CLUSTER_NAME not set"))

    val submitterConf = SubmitterConf(
      projectId,
      region,
      clusterName
    )
    val submitter = DataprocSubmitter(submitterConf)

    val jarUri = args.filter(_.startsWith(JAR_URI_ARG_PREFIX))(0).split("=")(1)
    val mainClass = args.filter(_.startsWith(MAIN_CLASS_PREFIX))(0).split("=")(1)
    val jobTypeValue = args.filter(_.startsWith(JOB_TYPE_ARG_PREFIX))(0).split("=")(1)

    val (dataprocJobType, jobProps) = jobTypeValue.toLowerCase match {
      case "spark" => (TypeSparkJob, Map(MainClass -> mainClass, JarURI -> jarUri))
      case "flink" => {
        val flinkMainJarUri = args.filter(_.startsWith(FLINK_MAIN_JAR_URI_ARG_PREFIX))(0).split("=")(1)
        val baseJobProps = Map(MainClass -> mainClass, JarURI -> jarUri, FlinkMainJarURI -> flinkMainJarUri)
        if (args.exists(_.startsWith(FLINK_SAVEPOINT_URI_ARG_PREFIX))) {
          val savepointUri = args.filter(_.startsWith(FLINK_SAVEPOINT_URI_ARG_PREFIX))(0).split("=")(1)
          (TypeFlinkJob, baseJobProps + (SavepointUri -> savepointUri))
        } else (TypeFlinkJob, baseJobProps)
      }
      case _ => throw new Exception("Invalid job type")
    }

    val finalArgs = dataprocJobType match {
      case TypeSparkJob => {
        val bigtableInstanceId = sys.env.getOrElse("GCP_BIGTABLE_INSTANCE_ID", "")
        val gcpArgsToPass = Array.apply(
          "--is-gcp",
          s"--gcp-project-id=${projectId}",
          s"--gcp-bigtable-instance-id=$bigtableInstanceId"
        )
        Array.concat(userArgs, gcpArgsToPass)
      }
      case TypeFlinkJob => userArgs
    }

    println(finalArgs.mkString("Array(", ", ", ")"))

    val jobId = submitter.submit(
      dataprocJobType,
      jobProps,
      gcsFiles.toList,
      finalArgs: _*
    )
    println("Dataproc submitter job id: " + jobId)
  }
}

object DataprocAuth extends JobAuth {}
