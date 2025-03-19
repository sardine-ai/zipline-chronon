package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.JobAuth
import ai.chronon.spark.JobSubmitter
import ai.chronon.spark.JobSubmitterConstants._
import ai.chronon.spark.JobType
import ai.chronon.spark.{FlinkJob => TypeFlinkJob}
import ai.chronon.spark.{SparkJob => TypeSparkJob}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.yaml.shaded_snakeyaml.Yaml

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
        val flinkStateUri =
          jobProperties.getOrElse(FlinkStateUri, throw new RuntimeException(s"Missing expected $FlinkStateUri"))
        val maybeSavepointUri = jobProperties.get(SavepointUri)
        buildFlinkJob(mainClass, mainJarUri, jarUri, flinkStateUri, maybeSavepointUri, args: _*)
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
                            flinkStateUri: String,
                            maybeSavePointUri: Option[String],
                            args: String*): Job.Builder = {

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
        "taskmanager.memory.process.size" -> "32G",
        "taskmanager.memory.network.min" -> "512m",
        "taskmanager.memory.network.max" -> "1G",
        // explicitly set the number of task slots as otherwise it defaults to the number of cores
        // we go with one task slot per TM as we do see issues with Spark setting updates not being respected when there's multiple slots/TM
        "taskmanager.numberOfTaskSlots" -> "1",
        "taskmanager.memory.managed.fraction" -> "0.5f",
        // default is 256m, we seem to be close to the limit so we give ourselves some headroom
        "taskmanager.memory.jvm-metaspace.size" -> "512m",
        // bump this a bit as Kafka and KV stores often need direct buffers
        "taskmanager.memory.task.off-heap.size" -> "512m",
        "yarn.classpath.include-user-jar" -> "FIRST",
        "state.savepoints.dir" -> flinkStateUri,
        "state.checkpoints.dir" -> flinkStateUri,
        // override the local dir for rocksdb as the default ends up being too large file name size wise
        "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
        "state.checkpoint-storage" -> "filesystem",
        "rest.flamegraph.enabled" -> "true",
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

  // TODO: merge this with FilesArgKeyword
  private val GCSFilesArgKeyword = "--gcs-files"

  def main(args: Array[String]): Unit = {

    // search args array for prefix `--gcs_files`
    val gcsFilesArgs = args.filter(_.startsWith(GCSFilesArgKeyword))
    assert(gcsFilesArgs.length == 0 || gcsFilesArgs.length == 1)

    val gcsFiles = if (gcsFilesArgs.isEmpty) {
      Array.empty[String]
    } else {
      gcsFilesArgs(0).split("=")(1).split(",")
    }

    // List of args that are not application args
    val internalArgs = Set(JarUriArgKeyword,
                           JobTypeArgKeyword,
                           MainClassKeyword,
                           FlinkMainJarUriArgKeyword,
                           FlinkSavepointUriArgKeyword,
                           GCSFilesArgKeyword)
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

    val jarUri = args.filter(_.startsWith(JarUriArgKeyword))(0).split("=")(1)
    val mainClass = args.filter(_.startsWith(MainClassKeyword))(0).split("=")(1)
    val jobTypeValue = args.filter(_.startsWith(JobTypeArgKeyword))(0).split("=")(1)

    val (jobType, jobProps) = jobTypeValue.toLowerCase match {
      case "spark" => (TypeSparkJob, Map(MainClass -> mainClass, JarURI -> jarUri))
      case "flink" => {
        val flinkStateUri = sys.env.getOrElse("FLINK_STATE_URI", throw new Exception("FLINK_STATE_URI not set"))

        val flinkMainJarUri = args.filter(_.startsWith(FlinkMainJarUriArgKeyword))(0).split("=")(1)
        val baseJobProps = Map(MainClass -> mainClass,
                               JarURI -> jarUri,
                               FlinkMainJarURI -> flinkMainJarUri,
                               FlinkStateUri -> flinkStateUri)
        if (args.exists(_.startsWith(FlinkSavepointUriArgKeyword))) {
          val savepointUri = args.filter(_.startsWith(FlinkSavepointUriArgKeyword))(0).split("=")(1)
          (TypeFlinkJob, baseJobProps + (SavepointUri -> savepointUri))
        } else (TypeFlinkJob, baseJobProps)
      }
      case _ => throw new Exception("Invalid job type")
    }

    val finalArgs = jobType match {
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
      jobType,
      jobProps,
      gcsFiles.toList,
      finalArgs: _*
    )
    println("Dataproc submitter job id: " + jobId)
    println(
      s"Safe to exit. Follow the job status at: https://console.cloud.google.com/dataproc/jobs/${jobId}/configuration?region=${region}&project=${projectId}")
  }
}

object DataprocAuth extends JobAuth {}
