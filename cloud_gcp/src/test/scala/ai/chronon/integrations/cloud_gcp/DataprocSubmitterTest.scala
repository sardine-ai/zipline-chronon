package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark
import ai.chronon.spark.submission.JobSubmitterConstants.FlinkMainJarURI
import ai.chronon.spark.submission.JobSubmitterConstants.FlinkStateUri
import ai.chronon.spark.submission.JobSubmitterConstants.JarURI
import ai.chronon.spark.submission.JobSubmitterConstants.MainClass
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.dataproc.v1._
import com.google.cloud.dataproc.v1.stub.JobControllerStub
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
      submitter.submit(spark.submission.SparkJob,
                       Map(MainClass -> "test-main-class", JarURI -> "test-jar-uri"),
                       Map.empty,
                       List.empty)
    assertEquals(submittedJobId, jobId)
  }

  it should "test flink job locally" ignore {

    val submitter = DataprocSubmitter()
    submitter.submit(
      spark.submission.FlinkJob,
      Map(
        MainClass -> "ai.chronon.flink.FlinkJob",
        FlinkMainJarURI -> "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
        // Include savepoint / checkpoint Uri to resume from where a job left off
        // SavepointUri -> "gs://zl-warehouse/flink-state/93686c72c3fd63f58d631e8388d8180d/chk-12",
        JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
        // This is where we write out checkpoints / persist state while the job is running
        FlinkStateUri -> "gs://zl-warehouse/flink-state"
      ),
      Map.empty,
      List.empty,
      "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
      "--groupby-name=listing_canary.actions_v1",
      "--kafka-bootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
      "-ZGCP_PROJECT_ID=canary-443022",
      "-ZGCP_BIGTABLE_INSTANCE_ID=zipline-canary-instance"
    )
  }

  it should "test flink kafka ingest job locally" ignore {

    val submitterConf = SubmitterConf("canary-443022", "us-central1", "zipline-canary-cluster")
    val submitter = DataprocSubmitter(submitterConf)
    val submittedJobId =
      submitter.submit(
        spark.submission.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink.FlinkKafkaItemEventDriver",
          FlinkMainJarURI -> "gs://zipline-jars/flink_kafka_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
          // This is where we write out checkpoints / persist state while the job is running
          FlinkStateUri -> "gs://zl-warehouse/flink-state"
        ),
        Map.empty,
        List.empty,
        "--kafka-bootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
        "--kafka-topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10",
      )
    println(submittedJobId)
  }

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {

    val submitter = DataprocSubmitter()
    val submittedJobId =
      submitter.submit(
        spark.submission.SparkJob,
        Map(MainClass -> "ai.chronon.spark.Driver",
            JarURI -> "gs://zipline-jars/cloud_gcp-assembly-0.1.0-SNAPSHOT.jar"),
        Map.empty,
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
        spark.submission.SparkJob,
        Map(MainClass -> "ai.chronon.spark.Driver",
            JarURI -> "gs://zipline-jars/cloud_gcp-assembly-0.1.0-SNAPSHOT.jar"),
        Map.empty,
        List.empty,
        "groupby-upload-bulk-load",
        "-ZGCP_PROJECT_ID=bigtable-project-id",
        "-ZGCP_INSTANCE_ID=bigtable-instance-id",
        "--online-jar=cloud_gcp-assembly-0.1.0-SNAPSHOT.jar",
        "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
        "--src-offline-table=data.test_gbu",
        "--group-by-name=quickstart.purchases.v1",
        "--partition-string=2024-01-01"
      )
    println(submittedJobId)
    assertEquals(submittedJobId, "mock-job-id")
  }
}
