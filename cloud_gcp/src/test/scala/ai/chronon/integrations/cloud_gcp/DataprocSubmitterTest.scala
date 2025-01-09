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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class DataprocSubmitterTest extends AnyFunSuite with MockitoSugar {

  test("DataprocClient should return job id when a job is submitted") {

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

    val submitter = new DataprocSubmitter(
      mockJobControllerClient,
      SubmitterConf("test-project", "test-region", "test-cluster"))

    val submittedJobId = submitter.submit(spark.SparkJob, Map(MainClass -> "test-main-class", JarURI -> "test-jar-uri"), List.empty)
    assertEquals(submittedJobId, jobId)
  }

  test("Verify classpath with spark-bigquery-connector") {
    BigQueryUtilScala.validateScalaVersionCompatibility()
  }

  ignore("test flink job locally") {
    val submitter = DataprocSubmitter()
    val submittedJobId =
      submitter.submit(spark.FlinkJob,
        Map(MainClass -> "ai.chronon.flink.FlinkJob",
          FlinkMainJarURI -> "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar"),
        List.empty,
        "--online-class=ai.chronon.integrations.cloud_gcp.GcpApiImpl",
        "--groupby-name=e2e-count",
        "-ZGCP_PROJECT_ID=canary-443022",
        "-ZGCP_INSTANCE_ID=zipline-canary-instance")
    println(submittedJobId)
  }

  ignore("Used to iterate locally. Do not enable this in CI/CD!") {

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

  ignore("Used to test GBU bulk load locally. Do not enable this in CI/CD!") {

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
        "--partition-string=2024-01-01")
    println(submittedJobId)
    assertEquals(submittedJobId, "mock-job-id")
  }

}
