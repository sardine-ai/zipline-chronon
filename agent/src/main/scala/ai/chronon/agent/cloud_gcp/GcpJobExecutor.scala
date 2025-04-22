package ai.chronon.agent.cloud_gcp

import ai.chronon.agent.JobExecutor
import ai.chronon.api.{JobStatusType, YarnClusterSpec, YarnJobType, Job => ZiplineJob}
import ai.chronon.integrations.cloud_gcp.{DataprocSubmitter, SubmitterConf}
import ai.chronon.spark.submission.{FlinkJob => TypeFlinkJob, SparkJob => TypeSparkJob}
import com.google.cloud.dataproc.v1._
import com.google.protobuf.Duration

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable

class GcpJobExecutor(
    region: String,
    projectId: String,
    customerId: String,
    // Inject clients
    clusterControllerClientFactory: String => ClusterControllerClient =
      GcpJobExecutor.defaultClusterControllerClientFactory,
    autoscalingClientFactory: String => AutoscalingPolicyServiceClient = GcpJobExecutor.defaultAutoscalingClientFactory,
    jobControllerClientFactory: String => JobControllerClient = GcpJobExecutor.defaultJobControllerClientFactory,
    dataprocSubmitterFactory: SubmitterConf => DataprocSubmitter = GcpJobExecutor.defaultDataprocSubmitterFactory
) extends JobExecutor {
  // Validate region and projectId
  require(region != null && region.nonEmpty, "Region must be specified and cannot be empty.")
  require(projectId != null && projectId.nonEmpty, "Project ID must be specified and cannot be empty.")
  require(customerId != null && customerId.nonEmpty, "Customer ID must be specified and cannot be empty.")

  def buildClusterConfig(clusterSpec: YarnClusterSpec): ClusterConfig = {
    val autoscalingConfig = if (clusterSpec.isSet(YarnClusterSpec._Fields.AUTO_SCALING_SPEC)) {
      val autoscalingClient: AutoscalingPolicyServiceClient = autoscalingClientFactory(region)
      val autoscalingPolicy = AutoscalingPolicy
        .newBuilder()
        .setWorkerConfig(
          InstanceGroupAutoscalingPolicyConfig
            .newBuilder()
            .setMinInstances(clusterSpec.autoScalingSpec.minInstances)
            .setMaxInstances(clusterSpec.autoScalingSpec.minInstances)
            .build()
        )
        .setBasicAlgorithm(
          BasicAutoscalingAlgorithm
            .newBuilder()
            .setCooldownPeriod(
              Duration.newBuilder().setSeconds(clusterSpec.autoScalingSpec.cooldownPeriod.toInt)
            ) // Adjust cooldown period as needed
            .setYarnConfig(
              BasicYarnAutoscalingConfig
                .newBuilder()
                .setScaleUpFactor(clusterSpec.autoScalingSpec.scaleUpFactor)
                .setScaleDownFactor(clusterSpec.autoScalingSpec.scaleDownFactor)
                .build()
            )
            .build()
        )
        .build()
      val regionName = RegionName
        .newBuilder()
        .setRegion(region)
        .setProject(projectId)
        .build()

      val policy = autoscalingClient.createAutoscalingPolicy(regionName, autoscalingPolicy)

      AutoscalingConfig
        .newBuilder()
        .setPolicyUri(
          s"projects/$projectId/regions/$region/autoscalingPolicies/${policy.getName}"
        )
        .build()

    } else {
      AutoscalingConfig
        .newBuilder()
        .setPolicyUri(
          s"projects/$projectId/regions/$region/autoscalingPolicies/zipline-$customerId-autoscaling-policy"
        )
        .build()

    }

    // Build the cluster configuration with autoscaling
    ClusterConfig
      .newBuilder()
      .setMasterConfig(
        InstanceGroupConfig
          .newBuilder()
          .setNumInstances(1)
          .setMachineTypeUri("n1-standard-4") // Adjust machine type as needed
          .setDiskConfig(
            DiskConfig
              .newBuilder()
              .setBootDiskType("pd-standard") // Use SSD for better performance
              .setBootDiskSizeGb(1024) // Adjust disk size as needed
              .build()
          )
          .build()
      )
      .setWorkerConfig(
        InstanceGroupConfig
          .newBuilder()
          .setNumInstances(2) // Initial number of worker nodes. Autoscaling will adjust this
          .setMachineTypeUri(clusterSpec.hostType)
          .setDiskConfig(
            DiskConfig
              .newBuilder()
              .setBootDiskType("pd-standard")
              .setBootDiskSizeGb(64)
              .setNumLocalSsds(2)
              .build()
          )
          .build()
      )
      .setAutoscalingConfig(autoscalingConfig) // Attach autoscaling config
      .setGceClusterConfig(
        GceClusterConfig
          .newBuilder()
          .setNetworkUri(clusterSpec.networkPolicy) // Specify the zone for the cluster
          .setServiceAccount(f"dataproc@$projectId.iam.gserviceaccount.com")
          .addAllServiceAccountScopes(
            List(
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
              "https://www.googleapis.com/auth/devstorage.read_write",
              "https://www.googleapis.com/auth/logging.write"
            )
          )
          .putMetadata("hive-version", "3.1.2")
          .putMetadata("SPARK_BQ_CONNECTOR_URL", "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar")
          .setInternalIpOnly(true)
          .build()
      )
      .setSoftwareConfig(
        SoftwareConfig
          .newBuilder()
          .setImageVersion(clusterSpec.yarnOfferingVersion) // Specify the image version for the cluster
          .addOptionalComponents(Component.FLINK)
          .addOptionalComponents(Component.JUPYTER)
          .putProperties("flink:env.java.opts.client",
                         "-Djava.net.preferIPv4Stack=true -Djava.security.properties=/etc/flink/conf/java.security")
          .build()
      )
      .addInitializationActions(
        NodeInitializationAction
          .newBuilder()
          .setExecutableFile(f"gs://zipline-artifacts-$customerId/scripts/copy_java_security.sh")
          .build()
      )
      .addInitializationActions(NodeInitializationAction
        .newBuilder()
        .setExecutableFile(f"gs://zipline-artifacts-$customerId/scripts/opsagent_install.sh")
        .build())
      .setEndpointConfig(
        EndpointConfig
          .newBuilder()
          .setEnableHttpPortAccess(true)
          .build()
      )
      .build()
  }

  def createDataprocCluster(clusterSpec: YarnClusterSpec): String = {
    val clusterConfig = buildClusterConfig(clusterSpec)

    val cluster: Cluster = Cluster
      .newBuilder()
      .setClusterName(clusterSpec.clusterName)
      .setProjectId(projectId)
      .setConfig(clusterConfig)
      .build()

    val dataprocClient = clusterControllerClientFactory(region)
    val clusterName = cluster.getClusterName

    val createRequest = CreateClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setCluster(cluster)
      .build()

    // Asynchronously create the cluster and wait for it to be ready
    dataprocClient
      .createClusterAsync(createRequest)
      .get(5, java.util.concurrent.TimeUnit.MINUTES) match {
      case null =>
        throw new RuntimeException("Failed to create Dataproc cluster.")
      case _ =>
        println(s"Created Dataproc cluster: $clusterName")
        clusterName
    }
    // Check status of the cluster creation
    var currentState = dataprocClient.getCluster(projectId, region, clusterName).getStatus.getState
    while (
      currentState != ClusterStatus.State.RUNNING &&
      currentState != ClusterStatus.State.ERROR &&
      currentState != ClusterStatus.State.STOPPING
    ) {
      println(s"Waiting for Dataproc cluster $clusterName to be in RUNNING state. Current state: $currentState")
      Thread.sleep(30000) // Wait for 30 seconds before checking again
      currentState = dataprocClient.getCluster(projectId, region, clusterName).getStatus.getState
    }
    currentState match {
      case ClusterStatus.State.RUNNING =>
        println(s"Dataproc cluster $clusterName is running.")
        clusterName
      case ClusterStatus.State.ERROR =>
        throw new RuntimeException(s"Failed to create Dataproc cluster $clusterName: ERROR state.")
      case _ =>
        throw new RuntimeException(s"Dataproc cluster $clusterName is in unexpected state: $currentState.")
    }

  }

  def createSparkCluster(clusterSpec: YarnClusterSpec): String = {
    // Create a Dataproc cluster and return the cluster name
    createDataprocCluster(clusterSpec) // Call the helper function to create the cluster
  }

  def createFlinkCluster(clusterSpec: YarnClusterSpec): String = {
    // Check if a streaming Dataproc cluster already exists
    if (checkForStreamingDataprocCluster(clusterSpec.clusterName)) {
      println(s"Streaming Dataproc cluster already exists for customer $customerId.")
      return clusterSpec.clusterName // Return existing cluster name
    }

    createDataprocCluster(clusterSpec) // Call the helper function to create the cluster
  }

  def checkForStreamingDataprocCluster(clusterName: String): Boolean = {
    // Check if a streaming Dataproc cluster already exists
    val dataprocClient = clusterControllerClientFactory(region)
    try {
      val listClustersRequest = ListClustersRequest
        .newBuilder()
        .setProjectId(projectId)
        .setRegion(region)
        .build()

      val clusters = dataprocClient.listClusters(listClustersRequest).iterateAll().iterator()
      while (clusters.hasNext) {
        val cluster = clusters.next()
        if (cluster.getClusterName == clusterName) {
          println(s"Found existing streaming Dataproc cluster: ${cluster.getClusterName}")
          return true
        }
      }
      false
    } finally {
      dataprocClient.close()
    }
  }

  def deleteDataprocCluster(clusterName: String): Unit = {
    val dataprocClient = clusterControllerClientFactory(region)

    val deleteRequest = DeleteClusterRequest
      .newBuilder()
      .setProjectId(projectId)
      .setRegion(region)
      .setClusterName(clusterName)
      .build()

    try {
      dataprocClient
        .deleteClusterCallable()
        .futureCall(deleteRequest)
        .get(5, java.util.concurrent.TimeUnit.MINUTES) // Wait for up to 5 minutes for deletion
      println(s"Deleted Dataproc cluster: $clusterName")
    } catch {
      case e: Exception =>
        println(s"Failed to delete Dataproc cluster $clusterName: ${e.getMessage}")
    } finally {
      dataprocClient.close()
    }
  }

  override def submitJob(job: ZiplineJob): Unit = {

    if (job.jobUnion.isSetYarnJob) {
      val yarnJob = job.jobUnion.getYarnJob

      val files = (for ((fileName, fileContents) <- yarnJob.fileWithContents) yield {
        val filePath = s"/tmp/$fileName"
        val file = new java.io.File(filePath)
        val writer = new java.io.PrintWriter(file)
        try {
          writer.write(fileContents)
        } finally {
          writer.close()
        }
        filePath
      }).toList
      val args = yarnJob.argsList

      // Implement logic to submit a Spark or Flink job to Dataproc
      var submissionProperties = mutable.Map(
        "mainClass" -> "ai.chronon.spark.Driver",
        "jarURI" -> "gs://zipline-jars/spark-assembly-0.1.0-SNAPSHOT.jar"
      )
      if (yarnJob.jobType == YarnJobType.SPARK) {
        val jobProperties = Map(
          "spark.app.name" -> yarnJob.appName,
          "spark.master" -> s"yarn",
          "spark.submit.deployMode" -> "cluster"
        )
        val clusterId = createSparkCluster(yarnJob.clusterSpec)
        val jobId = submitSparkJob(clusterId, submissionProperties.toMap, jobProperties, files, args: _*)
        println(s"Spark job submitted with ID: $jobId")
      } else if (yarnJob.jobType == YarnJobType.FLINK) {
        submissionProperties ++= Map(
          "flinkMainJarURI" -> "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
          "flinkStateUri" -> "gs://zl-warehouse/flink-state"
        )
        val jobProperties = Map(
          "flink.app.name" -> yarnJob.appName,
          "flink.master" -> s"yarn-cluster"
        )
        val clusterId = createFlinkCluster(yarnJob.clusterSpec)
        val jobId = submitFlinkJob(clusterId, submissionProperties.toMap, jobProperties, files, args: _*)
        println(s"Flink job submitted with ID: $jobId")
      } else {
        throw new IllegalArgumentException(s"Unsupported job type: ${yarnJob.jobType}")
      }

    }
  }

  def submitSparkJob(clusterId: String,
                     submissionProperties: Map[String, String],
                     jobProperties: Map[String, String],
                     files: List[String],
                     args: String*): String = {
    // Ensure the cluster is created before submitting the job
    val conf = SubmitterConf(projectId, region, clusterId)
    val dataprocSubmitter = dataprocSubmitterFactory(conf)

    dataprocSubmitter.submit(TypeSparkJob, submissionProperties, jobProperties, files, args: _*)
  }

  def submitFlinkJob(clusterId: String,
                     submissionProperties: Map[String, String],
                     jobProperties: Map[String, String],
                     files: List[String],
                     args: String*): String = {
    // Create cluster before submitting the job
    val conf = SubmitterConf(projectId, region, clusterId)
    val dataprocSubmitter = dataprocSubmitterFactory(conf)

    dataprocSubmitter.submit(TypeFlinkJob, submissionProperties, jobProperties, files, args: _*)
  }

  override def getJobStatus(jobId: String): JobStatusType = {
    // Implement logic to get the status of a Spark job
    println(s"Getting status for job with ID: $jobId")
    val jobControllerClient = jobControllerClientFactory(region)
    val job = jobControllerClient.getJob(
      GetJobRequest
        .newBuilder()
        .setProjectId(projectId)
        .setRegion(region)
        .setJobId(jobId)
        .build()
    )
    JobStatusType.valueOf(job.getStatus.getState.name())
  }

  def killJob(jobId: String): Unit = {
    // Implement logic to kill a Spark job if needed
    println(s"Killing dataproc job with ID: $jobId")
    val jobControllerClient = jobControllerClientFactory(region)
    jobControllerClient.cancelJob(
      CancelJobRequest
        .newBuilder()
        .setProjectId(projectId)
        .setRegion(region)
        .setJobId(jobId)
        .build()
    )
  }
}

object GcpJobExecutor {
  private def defaultClusterControllerClientFactory(region: String): ClusterControllerClient = {
    ClusterControllerClient.create(
      ClusterControllerSettings.newBuilder().setEndpoint(s"$region-dataproc.googleapis.com:443").build())
  }
  private def defaultAutoscalingClientFactory(region: String): AutoscalingPolicyServiceClient = {
    AutoscalingPolicyServiceClient.create(
      AutoscalingPolicyServiceSettings.newBuilder().setEndpoint(s"$region-dataproc.googleapis.com:443").build())
  }
  private def defaultJobControllerClientFactory(region: String): JobControllerClient = {
    JobControllerClient.create(
      JobControllerSettings.newBuilder().setEndpoint(s"$region-dataproc.googleapis.com:443").build())
  }
  private def defaultDataprocSubmitterFactory(conf: SubmitterConf): DataprocSubmitter = {
    DataprocSubmitter(conf)
  }
}
