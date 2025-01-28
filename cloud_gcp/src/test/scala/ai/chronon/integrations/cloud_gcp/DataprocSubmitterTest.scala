package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark
import ai.chronon.spark.JobSubmitterConstants.FlinkMainJarURI
import ai.chronon.spark.JobSubmitterConstants.JarURI
import ai.chronon.spark.JobSubmitterConstants.MainClass
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.dataproc.v1._
import com.google.cloud.dataproc.v1.stub.JobControllerStub
import com.google.cloud.spark.bigquery.BigQueryUtilScala
import org.junit.Assert.assertEquals
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class DataprocSubmitterTest extends AnyFlatSpec with MockitoSugar {

  "DataprocClient" should "return job id when a job is submitted" in {

    // Mock dataproc job client.
    val jobId = "mock-job-id"
    val mockJob = Job
      .newBuilder()
      .setReference(JobReference.newBuilder().setJobId(jobId))
      .setStatus(JobStatus.newBuilder().setState(JobStatus.State.DONE))
      .build()

    val mockJobControllerStub = mock[JobControllerStub]
    val mockSubmitJobCallable = mock[UnaryCallable[SubmitJobRequest, Job]]

    when(mockSubmitJobCallable.call(any()))
      .thenReturn(mockJob)

    when(mockJobControllerStub.submitJobCallable)
      .thenReturn(mockSubmitJobCallable)

    val mockJobControllerClient = JobControllerClient.create(mockJobControllerStub)

    // Test starts here.

    val submitter =
      new DataprocSubmitter(mockJobControllerClient, SubmitterConf("test-project", "test-region", "test-cluster"))

    val submittedJobId =
      submitter.submit(spark.SparkJob, Map(MainClass -> "test-main-class", JarURI -> "test-jar-uri"), List.empty)
    assertEquals(submittedJobId, jobId)
  }

  it should "Verify classpath with spark-bigquery-connector" in {
    BigQueryUtilScala.validateScalaVersionCompatibility()
  }

  it should "test flink job locally" ignore {

    val submitter = DataprocSubmitter()
    submitter.submit(
      spark.FlinkJob,
      Map(
        MainClass -> "ai.chronon.flink.FlinkJob",
        FlinkMainJarURI -> "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
        JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar"
      ),
      List.empty,
      "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
      "--groupby-name=e2e-count",
      "-ZGCP_PROJECT_ID=bigtable-project-id",
      "-ZGCP_INSTANCE_ID=bigtable-instance-id"
    )
  }

  it should "test flink kafka ingest job locally" ignore {

    val submitter = DataprocSubmitter()
    val submittedJobId =
      submitter.submit(
        spark.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink.FlinkKafkaBeaconEventDriver",
          FlinkMainJarURI -> "gs://zipline-jars/flink_kafka_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar"
        ),
        List.empty,
        "--kafka-bootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
        "--kafka-topic=test-beacon-main",
        "--data-file-name=gs://zl-warehouse/beacon_events/beacon-output.avro"
      )
    println(submittedJobId)
  }

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {

    val submitter = DataprocSubmitter()
    val submittedJobId =
      submitter.submit(
        spark.SparkJob,
        Map(MainClass -> "ai.chronon.spark.Driver",
            JarURI -> "gs://zipline-jars/cloud_gcp-assembly-0.1.0-SNAPSHOT.jar"),
        List("gs://zipline-jars/training_set.v1",
             "gs://zipline-jars/dataproc-submitter-conf.yaml",
             "gs://zipline-jars/additional-confs.yaml"),
        "join",
        "--end-date=2024-12-10",
        "--additional-conf-path=additional-confs.yaml",
        "--conf-path=training_set.v1"
      )
    println(submittedJobId)
  }

  it should "Used to test GBU bulk load locally. Do not enable this in CI/CD!" ignore {

    val submitter = DataprocSubmitter()
    val submittedJobId =
      submitter.submit(
        spark.SparkJob,
        Map(MainClass -> "ai.chronon.spark.Driver",
            JarURI -> "gs://zipline-jars/cloud_gcp-assembly-0.1.0-SNAPSHOT.jar"),
        List.empty,
        "groupby-upload-bulk-load",
        "-ZGCP_PROJECT_ID=bigtable-project-id",
        "-ZGCP_INSTANCE_ID=bigtable-instance-id",
        "--online-jar=cloud_gcp-assembly-0.1.0-SNAPSHOT.jar",
        "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
        "--src-offline-table=data.test_gbu",
        "--groupby-name=quickstart.purchases.v1",
        "--partition-string=2024-01-01"
      )
    println(submittedJobId)
    assertEquals(submittedJobId, "mock-job-id")
  }
}
