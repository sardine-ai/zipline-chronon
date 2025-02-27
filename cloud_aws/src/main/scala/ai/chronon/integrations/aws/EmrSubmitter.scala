package ai.chronon.integrations.aws

import ai.chronon.integrations.aws.EmrSubmitter.DefaultClusterIdleTimeout
import ai.chronon.integrations.aws.EmrSubmitter.DefaultClusterInstanceCount
import ai.chronon.integrations.aws.EmrSubmitter.DefaultClusterInstanceType
import ai.chronon.spark.JobSubmitter
import ai.chronon.spark.JobSubmitterConstants._
import ai.chronon.spark.JobType
import ai.chronon.spark.SparkJob
import ai.chronon.spark.{SparkJob => TypeSparkJob}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.Application
import software.amazon.awssdk.services.emr.model.AutoTerminationPolicy
import software.amazon.awssdk.services.emr.model.BootstrapActionConfig
import software.amazon.awssdk.services.emr.model.CancelStepsRequest
import software.amazon.awssdk.services.emr.model.Configuration
import software.amazon.awssdk.services.emr.model.DescribeStepRequest
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest
import software.amazon.awssdk.services.emr.model.ScriptBootstrapActionConfig
import software.amazon.awssdk.services.emr.model.StepConfig

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

class EmrSubmitter(customerId: String, emrClient: EmrClient) extends JobSubmitter {

  private val ClusterApplications = List(
    "Flink",
    "Zeppelin",
    "JupyterEnterpriseGateway",
    "Hive",
    "Hadoop",
    "Livy",
    "Spark"
  )

  private val EmrReleaseLabel = "emr-7.2.0"

  // Customer specific infra configurations
  private val CustomerToSubnetIdMap = Map(
    "canary" -> "subnet-085b2af531b50db44"
  )
  private val CustomerToSecurityGroupIdMap = Map(
    "canary" -> "sg-04fb79b5932a41298"
  )

  private val CopyS3FilesToMntScript = "s3://zipline-artifacts-canary/copy_s3_files.sh"

  override def submit(jobType: JobType,
                      jobProperties: Map[String, String],
                      files: List[String],
                      args: String*): String = {

    val runJobFlowRequestBuilder = RunJobFlowRequest
      .builder()
      .name(s"job-${java.util.UUID.randomUUID.toString}")

    // Cluster infra configurations:
    val customerSecurityGroupId = CustomerToSecurityGroupIdMap.getOrElse(
      customerId,
      throw new RuntimeException(s"No security group id found for $customerId"))
    runJobFlowRequestBuilder
      .autoTerminationPolicy(
        AutoTerminationPolicy
          .builder()
          .idleTimeout(jobProperties.getOrElse(ClusterIdleTimeout, s"$DefaultClusterIdleTimeout").toLong)
          .build())
      .configurations(
        Configuration.builder
          .classification("spark-hive-site")
          .properties(Map(
            "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").asJava)
          .build()
      )
      .applications(ClusterApplications.map(app => Application.builder().name(app).build()): _*)
      // TODO: Could make this generalizable. Have logs saved where users want it
      .logUri(s"s3://zipline-warehouse-${customerId}/emr/")
      .instances(
        JobFlowInstancesConfig
          .builder()
          // We may want to make master and slave instance types different in the future
          .masterInstanceType(jobProperties.getOrElse(ClusterInstanceType, DefaultClusterInstanceType))
          .slaveInstanceType(jobProperties.getOrElse(ClusterInstanceType, DefaultClusterInstanceType))
          // Hack: We hardcode the subnet ID and sg id for each customer of Zipline. The subnet gets created from
          // Terraform so we'll need to be careful that these don't get accidentally destroyed.
          .ec2SubnetId(
            CustomerToSubnetIdMap.getOrElse(customerId,
                                            throw new RuntimeException(s"No subnet id found for $customerId")))
          .emrManagedMasterSecurityGroup(customerSecurityGroupId)
          .emrManagedSlaveSecurityGroup(customerSecurityGroupId)
          .instanceCount(jobProperties.getOrElse(ClusterInstanceCount, DefaultClusterInstanceCount).toInt)
          .keepJobFlowAliveWhenNoSteps(true) // Keep the cluster alive after the job is done
          .build())
      // TODO: need to double check that this is how we want our role names to be
      .serviceRole(s"zipline_${customerId}_emr_service_role")
      .jobFlowRole(s"zipline_${customerId}_emr_profile")
      .releaseLabel(EmrReleaseLabel)

    // Add single step (spark job) to run:
    val sparkSubmitArgs =
      Seq("spark-submit",
          "--class",
          jobProperties(MainClass),
          jobProperties(JarURI)) ++ args // For EMR, we explicitly spark-submit the job
    val stepConfig = StepConfig
      .builder()
      .name("Zipline Job")
      .actionOnFailure("CANCEL_AND_WAIT") // want the cluster to not terminate if the step fails
      .hadoopJarStep(
        jobType match {
          case SparkJob =>
            HadoopJarStepConfig
              .builder()
              // Using command-runner.jar from AWS:
              // https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html
              .jar("command-runner.jar")
              .args(sparkSubmitArgs: _*)
              .build()
          // TODO: add flink
          case _ => throw new IllegalArgumentException("Unsupported job type")
        }
      )
      .build()
    runJobFlowRequestBuilder.steps(stepConfig)

    // Add bootstrap actions if any
    if (files.nonEmpty) {
      val bootstrapActionConfig = BootstrapActionConfig
        .builder()
        .name("EMR Submitter: Copy S3 Files")
        .scriptBootstrapAction(
          ScriptBootstrapActionConfig
            .builder()
            .path(CopyS3FilesToMntScript)
            .args(files: _*)
            .build())
        .build()
      runJobFlowRequestBuilder.bootstrapActions(bootstrapActionConfig)
    }

    val jobFlowResponse = emrClient.runJobFlow(
      runJobFlowRequestBuilder.build()
    )

    jobFlowResponse.jobFlowId()
  }

  override def status(jobId: String): Unit = {
    val describeStepResponse = emrClient.describeStep(DescribeStepRequest.builder().stepId(jobId).build())
    val status = describeStepResponse.step().status()
    println(status)
  }

  override def kill(jobId: String): Unit = {
    emrClient.cancelSteps(CancelStepsRequest.builder().stepIds(jobId).build())
  }
}

object EmrSubmitter {
  def apply(): EmrSubmitter = {
    val customerId = sys.env.getOrElse("CUSTOMER_ID", throw new Exception("CUSTOMER_ID not set")).toLowerCase

    new EmrSubmitter(customerId,
                     EmrClient
                       .builder()
                       .build())
  }

  private val ClusterInstanceTypeArgKeyword = "--cluster-instance-type"
  private val ClusterInstanceCountArgKeyword = "--cluster-instance-count"
  private val ClusterIdleTimeoutArgKeyword = "--cluster-idle-timeout"

  private val DefaultClusterInstanceType = "m5.xlarge"
  private val DefaultClusterInstanceCount = "3"
  private val DefaultClusterIdleTimeout = 60 * 60 * 24 * 2 // 2 days in seconds

  def main(args: Array[String]): Unit = {

    // List of args that are not application args
    val internalArgs = Set(
      JarUriArgKeyword,
      JobTypeArgKeyword,
      MainClassKeyword,
      FlinkMainJarUriArgKeyword,
      FlinkSavepointUriArgKeyword,
      ClusterInstanceTypeArgKeyword,
      ClusterInstanceCountArgKeyword,
      ClusterIdleTimeoutArgKeyword
    )

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jarUri =
      args.find(_.startsWith(JarUriArgKeyword)).map(_.split("=")(1)).getOrElse(throw new Exception("Jar URI not found"))
    val mainClass = args
      .find(_.startsWith(MainClassKeyword))
      .map(_.split("=")(1))
      .getOrElse(throw new Exception("Main class not found"))
    val jobTypeValue = args
      .find(_.startsWith(JobTypeArgKeyword))
      .map(_.split("=")(1))
      .getOrElse(throw new Exception("Job type not found"))
    val clusterInstanceType =
      args.find(_.startsWith(ClusterInstanceTypeArgKeyword)).map(_.split("=")(1)).getOrElse(DefaultClusterInstanceType)
    val clusterInstanceCount = args
      .find(_.startsWith(ClusterInstanceCountArgKeyword))
      .map(_.split("=")(1))
      .getOrElse(DefaultClusterInstanceCount)
    val clusterIdleTimeout = args
      .find(_.startsWith(ClusterIdleTimeoutArgKeyword))
      .map(_.split("=")(1))
      .getOrElse(DefaultClusterIdleTimeout.toString)

    val (jobType, jobProps) = jobTypeValue.toLowerCase match {
      case "spark" => {
        val baseProps = Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          ClusterInstanceType -> clusterInstanceType,
          ClusterInstanceCount -> clusterInstanceCount,
          ClusterIdleTimeout -> clusterIdleTimeout
        )
        (TypeSparkJob, baseProps)
      }
      // TODO: add flink
      case _ => throw new Exception("Invalid job type")
    }

    val finalArgs = userArgs

    val emrSubmitter = EmrSubmitter()
    val jobId = emrSubmitter.submit(
      jobType,
      jobProps,
      List.empty,
      finalArgs: _*
    )

    println("EMR job id: " + jobId)
    println(s"Safe to exit. Follow the job status at: https://console.aws.amazon.com/emr/home#/clusterDetails/$jobId")

  }
}
