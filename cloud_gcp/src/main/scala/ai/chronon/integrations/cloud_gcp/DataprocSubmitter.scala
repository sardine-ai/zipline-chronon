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

  private def buildFlinkJob(mainClass: String, mainJarUri: String, jarUri: String, args: String*): Job.Builder = {
    val checkpointsDir = "gs://chronon-checkpoints" // TODO choose the correct value!
    
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
      Map("jobmanager.memory.process.size" -> "4G",
          "taskmanager.memory.process.size" -> "64G",
          "taskmanager.memory.network.min" -> "1G",
          "taskmanager.memory.network.max" -> "2G",
          "taskmanager.memory.managed.fraction" -> "0.5f",
          "yarn.classpath.include-user-jar" -> "FIRST",
          "execution.checkpointing.savepoint-dir" -> checkpointsDir,
          "execution.checkpointing.dir" -> checkpointsDir)

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
}

object DataprocAuth extends JobAuth {}
