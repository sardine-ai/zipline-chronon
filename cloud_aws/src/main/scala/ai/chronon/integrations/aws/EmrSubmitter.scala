package ai.chronon.integrations.aws

import ai.chronon.spark.JobSubmitter
import ai.chronon.spark.JobSubmitterConstants.JarURI
import ai.chronon.spark.JobSubmitterConstants.MainClass
import ai.chronon.spark.JobType
import ai.chronon.spark.SparkJob
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.Application
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

class EmrSubmitter(emrClient: EmrClient) extends JobSubmitter{

  override def submit(jobType: JobType, jobProperties: Map[String, String], files: List[String], args: String*): String = {

    val stepConfig = StepConfig.builder()
      .name("Zipline Job")
      .actionOnFailure("CANCEL_AND_WAIT")
      .hadoopJarStep(
        jobType match {
          case SparkJob =>
            HadoopJarStepConfig.builder()
//              .mainClass(jobProperties.getOrElse(MainClass, throw new RuntimeException("Main class not found"))
//              )
              .jar("command-runner.jar") // https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html
              .args(args: _*)
              .build()
          case _ => throw new IllegalArgumentException("Unsupported job type")
        }
      ).build()

    val runJobFlowRequest = RunJobFlowRequest.builder()
      .name(s"job-${java.util.UUID.randomUUID.toString}")
      .bootstrapActions(BootstrapActionConfig.builder()
        .name("Install application files")
        .scriptBootstrapAction(ScriptBootstrapActionConfig.builder()
          .path("s3://zipline-artifacts-canary/copy_files.sh")
          .build())
        .build())
      .configurations(Configuration.builder
        .classification("spark-hive-site")
        .properties(Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").asJava)
        .build())
      .applications(
//        Flink 1.18.1, Zeppelin 0.10.1, JupyterEnterpriseGateway 2.6.0, Hive 3.1.3, Hadoop 3.3.6, Livy 0.8.0, Spark 3.5.1
        Application.builder()
          .name("Flink")
          .build(),
//        Application.builder()
//          .name("Zeppelin")
//          .version("0.10.1")
//          .build(),
//        Application.builder()
//          .name("JupyterEnterpriseGateway")
//          .version("2.6.0")
//          .build(),
        Application.builder()
          .name("Hive")
          .build(),
        Application.builder()
          .name("Hadoop")
          .build(),
//        Application.builder()
//          .name("Livy")
//          .version("0.8.0")
//          .build(),
        Application.builder()
          .name("Spark")
          .build()
      )
      .steps(stepConfig)
      .logUri("s3://zipline-warehouse-canary/emr/")
      .instances(
        JobFlowInstancesConfig.builder()
          .masterInstanceType("m5.xlarge")
          .slaveInstanceType("m5.xlarge")
          .ec2SubnetId("subnet-085b2af531b50db44")
          .emrManagedMasterSecurityGroup("sg-04fb79b5932a41298")
          .emrManagedSlaveSecurityGroup("sg-04fb79b5932a41298")
          .instanceCount(3)
          .keepJobFlowAliveWhenNoSteps(true)
      .build())
      .serviceRole("zipline_canary_emr_service_role")
      .jobFlowRole("zipline_canary_emr_profile")
      .releaseLabel("emr-7.2.0")
      .build()

    val jobFlowResponse = emrClient.runJobFlow(
      runJobFlowRequest
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
    val emrClient = EmrClient.builder()
      .build()

    new EmrSubmitter(emrClient)
  }



  def main(args: Array[String]): Unit = {
    val emrSubmitter = EmrSubmitter()
    val jobId = emrSubmitter.submit(SparkJob,
      Map.apply(
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar"
      ),
      List.empty,
      "spark-submit",
      "--class", "ai.chronon.spark.Driver",
      "--conf", "spark.files=s3://zipline-warehouse-canary/training_set.v1",
      "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
      "join",
      "--conf-path", "training_set.v1",
      "--end-date", "2025-02-25",
      "--conf-type", "joins",
      "--additional-conf-path","additional-confs.yaml"
//      "--driver-memory", "10G"
    )

    println(jobId)
//      .withArgs("spark-submit","--executor-memory","1g","--class","org.apache.spark.examples.SparkPi","/usr/lib/spark/examples/jars/spark-examples.jar","10");

    //    files: spark.yarn.dist.files
//    spark.files	 https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html
//
  }
}