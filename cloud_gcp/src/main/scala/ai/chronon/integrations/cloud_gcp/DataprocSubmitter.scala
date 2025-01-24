package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.JobAuth
import ai.chronon.spark.JobSubmitter
import ai.chronon.spark.JobSubmitterConstants.FlinkMainJarURI
import ai.chronon.spark.JobSubmitterConstants.JarURI
import ai.chronon.spark.JobSubmitterConstants.MainClass
import ai.chronon.spark.JobType
import ai.chronon.spark.{FlinkJob => TypeFlinkJob}
import ai.chronon.spark.{SparkJob => TypeSparkJob}
import com.google.api.gax.rpc.ApiException
import com.google.cloud.dataproc.v1._
import org.apache.spark.deploy.VisibleSparkSubmitArguments
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
      case TypeSparkJob => buildSparkJob(args: _*)
      case TypeFlinkJob =>
        val mainJarUri =
          jobProperties.getOrElse(FlinkMainJarURI, throw new RuntimeException(s"Missing expected $FlinkMainJarURI"))
        buildFlinkJob(mainClass, mainJarUri, jarUri, args: _*)
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

  /*
  private def toSparkDriverEnv(propertyKey: String) = s"spark.yarn.appMasterEnv.$propertyKey"

  private def toSparkExecEnv(propertyKey: String) = s"spark.executorEnv.$propertyKey"
   */
  private def stringToSeq(commaDelimited: String) = {
    // Copied from: https://github.com/apache/spark/pull/49062/files#diff-2ecc6aef4b0c50bbf146e6c0b3b8b2249375f06a83e2a224c7718cfc850c3af7L2802-L2804
    // which in later versions is available at: https://github.com/apache/spark/blob/c662441de3a4fc84e938e9211c77b5143b095842/common/utils/src/main/scala/org/apache/spark/util/SparkStringUtils.scala#L21-L22
    Option(commaDelimited).getOrElse("").split(",").map(_.trim()).filter(_.nonEmpty)
  }

  private def buildSparkJob(args: String*): Job.Builder = {

    val sparkArgs = new VisibleSparkSubmitArguments(args.toSeq)

    val sparkJob = SparkJob
      .newBuilder()
      .setMainClass(sparkArgs.mainClass)
      .addAllJarFileUris(stringToSeq(sparkArgs.jars).toList.asJava)
      .addAllFileUris(stringToSeq(sparkArgs.files).toList.asJava)
      .addAllArgs(sparkArgs.childArgs.asJava)
      .putAllProperties(sparkArgs.sparkProperties.asJava)
      .build()
    Job.newBuilder().setSparkJob(sparkJob)
  }

  private def buildFlinkJob(mainClass: String, mainJarUri: String, jarUri: String, args: String*): Job.Builder = {

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

    val flinkJob = FlinkJob
      .newBuilder()
      .setMainClass(mainClass)
      .setMainJarFileUri(mainJarUri)
      .putAllProperties(envProps.asJava)
      .addJarFileUris(jarUri)
      .addAllArgs(args.toIterable.asJava)
      .build()
    Job.newBuilder().setFlinkJob(flinkJob)
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
    val gcsFilesArgs = args.filter(_.startsWith("--gcs_files"))
    assert(gcsFilesArgs.length == 0 || gcsFilesArgs.length == 1)

    val gcsFiles = if (gcsFilesArgs.isEmpty) {
      Array.empty[String]
    } else {
      gcsFilesArgs(0).split("=")(1).split(",")
    }

    val userArgs = args.filter(f => !f.startsWith("--gcs_files") && !f.startsWith("--chronon_jar_uri"))

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

    val bigtableInstanceId = sys.env.getOrElse("GCP_BIGTABLE_INSTANCE_ID", "")

    val gcpArgsToPass = Array.apply(
      "--is-gcp",
      s"--gcp-project-id=${projectId}",
      s"--gcp-bigtable-instance-id=$bigtableInstanceId"
    )

    val finalArgs = Array.concat(userArgs, gcpArgsToPass)

    println(finalArgs.mkString("Array(", ", ", ")"))

    val a = DataprocSubmitter(submitterConf)

    val jobId = a.submit(
      TypeSparkJob,
      Map(MainClass -> "ai.chronon.spark.Driver", JarURI -> chrononJarUri),
      gcsFiles.toList,
      finalArgs: _*
    )
    println("Dataproc submitter job id: " + jobId)
  }
}

object DataprocAuth extends JobAuth {}
