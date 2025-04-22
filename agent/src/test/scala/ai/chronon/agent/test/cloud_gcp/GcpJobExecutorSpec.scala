package ai.chronon.agent.test.cloud_gcp

import ai.chronon.agent.cloud_gcp.GcpJobExecutor
import ai.chronon.api.{
  JobBase,
  JobStatusType,
  YarnAutoScalingSpec,
  YarnClusterSpec,
  YarnJob,
  YarnJobType,
  Job => ZiplineJob
}
import ai.chronon.integrations.cloud_gcp.{DataprocSubmitter, SubmitterConf}
import ai.chronon.spark.submission.{FlinkJob => TypeFlinkJob, SparkJob => TypeSparkJob}
import com.google.api.core.ApiFuture
import com.google.api.gax.longrunning.{OperationFuture, OperationSnapshot}
import com.google.api.gax.retrying.RetryingFuture
import com.google.cloud.dataproc.v1.ClusterControllerClient.ListClustersPagedResponse
import com.google.longrunning.Operation
import com.google.cloud.dataproc.v1._
import com.google.protobuf.Duration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

import java.util.Optional
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

class GcpJobExecutorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  // Test data
  val testRegion = "test-region"
  val testProjectId = "test-project"
  val testCustomerId = "test-customer"
  val testClusterName = "test-cluster"
  val testJobId = "test-job-123"

  // Mocks
  val mockClusterClient = mock[ClusterControllerClient]
  val mockAutoscalingClient = mock[AutoscalingPolicyServiceClient]
  val mockJobClient = mock[JobControllerClient]
  val mockDataprocSubmitter = mock[DataprocSubmitter]

  // Helper to create test instance
  def createTestExecutor(
      region: String = testRegion,
      projectId: String = testProjectId,
      customerId: String = testCustomerId,
      clusterClient: String => ClusterControllerClient = _ => mockClusterClient,
      autoscalingClient: String => AutoscalingPolicyServiceClient = _ => mockAutoscalingClient,
      jobClient: String => JobControllerClient = _ => mockJobClient,
      submitterFactory: SubmitterConf => DataprocSubmitter = _ => mockDataprocSubmitter
  ): GcpJobExecutor = {
    reset(mockClusterClient)
    reset(mockAutoscalingClient)
    reset(mockJobClient)
    reset(mockDataprocSubmitter)
    new GcpJobExecutor(
      region,
      projectId,
      customerId,
      clusterClient,
      autoscalingClient,
      jobClient,
      submitterFactory
    )
  }

  "GcpJobExecutor constructor" should "validate parameters" in {
    an[IllegalArgumentException] should be thrownBy createTestExecutor(region = "")
    an[IllegalArgumentException] should be thrownBy createTestExecutor(projectId = "")
    an[IllegalArgumentException] should be thrownBy createTestExecutor(customerId = "")
  }

  "buildClusterConfig" should "create proper cluster configuration" in {
    val executor = createTestExecutor()

    // Create a real YarnClusterSpec instead of mocking
    val clusterSpec = new YarnClusterSpec()
      .setHostType("n1-standard-8")
      .setNetworkPolicy("default-network")
      .setYarnOfferingVersion("2.0")
      .setClusterName(testClusterName)

    val config = executor.buildClusterConfig(clusterSpec)

    config.getMasterConfig.getNumInstances shouldBe 1
    config.getWorkerConfig.getNumInstances shouldBe 2
    config.getGceClusterConfig.getNetworkUri shouldBe "default-network"
    config.getSoftwareConfig.getImageVersion shouldBe "2.0"
  }

  "buildClusterConfig" should "handle autoscaling when specified" in {
    val executor = createTestExecutor()

    val autoScalingSpec = new YarnAutoScalingSpec()
      .setMinInstances(2)
      .setScaleUpFactor(0.5)
      .setScaleDownFactor(0.1)
      .setCooldownPeriod("300")

    val clusterSpec = new YarnClusterSpec()
      .setAutoScalingSpec(autoScalingSpec)
      .setHostType("n1-standard-8")
      .setClusterName(testClusterName)
      .setNetworkPolicy("default-network")
      .setYarnOfferingVersion("dataproc-2.0")

    // Mock autoscaling policy creation
    val mockPolicy = mock[AutoscalingPolicy]
    when(mockAutoscalingClient.createAutoscalingPolicy(any[RegionName], any())).thenReturn(mockPolicy)
    when(mockPolicy.getName).thenReturn("test-policy")

    val config = executor.buildClusterConfig(clusterSpec)

    config.getAutoscalingConfig.getPolicyUri should include("test-policy")
    verify(mockAutoscalingClient).createAutoscalingPolicy(any[RegionName], any())
  }

  "createDataprocCluster" should "create and wait for cluster" in {
    val executor = createTestExecutor()

    // Create a real YarnClusterSpec instead of mocking
    val clusterSpec = new YarnClusterSpec()
      .setHostType("n1-standard-8")
      .setNetworkPolicy("default-network")
      .setYarnOfferingVersion("2.0")
      .setClusterName(testClusterName)

    // Mock cluster creation
    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setClusterName(testClusterName)
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockClusterClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockClusterClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)

    val clusterName = executor.createDataprocCluster(clusterSpec)

    clusterName shouldBe testClusterName
    verify(mockClusterClient, atLeastOnce()).getCluster(testProjectId, testRegion, testClusterName)
  }

  "submitJob" should "handle Spark jobs correctly" in {
    val executor = createTestExecutor()

    // Create a real YarnClusterSpec
    val clusterSpec = new YarnClusterSpec()
      .setHostType("n1-standard-8")
      .setNetworkPolicy("default-network")
      .setYarnOfferingVersion("2.0")
      .setClusterName(testClusterName)

    // Create a real YarnJob with all required fields
    val yarnJob = new YarnJob()
      .setJobType(YarnJobType.SPARK)
      .setAppName("test-app")
      .setArgsList(List("arg1", "arg2").asJava)
      .setFileWithContents(Map("file1" -> "content1").asJava)
      .setClusterSpec(clusterSpec)

    // Create a real ZiplineJob with the YarnJob set
    val ziplineJob = new ZiplineJob()
    val jobUnion = new JobBase()
    jobUnion.setYarnJob(yarnJob)
    ziplineJob.setJobUnion(jobUnion)

    // Mock cluster creation
    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setClusterName(testClusterName)
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockClusterClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockClusterClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)
    // Mock job submission
    when(
      mockDataprocSubmitter.submit(
        any[TypeSparkJob.type],
        any[Map[String, String]],
        any[Map[String, String]],
        any[List[String]],
        any[String]
      )).thenReturn(testJobId)

    // Execute and verify
    executor.submitJob(ziplineJob)

    verify(mockDataprocSubmitter).submit(
      any[TypeSparkJob.type],
      any[Map[String, String]],
      any[Map[String, String]],
      any[List[String]],
      any[String]
    )
  }

  "submitJob" should "handle Flink jobs correctly" in {
    val executor = createTestExecutor()

    // Create a real YarnClusterSpec
    val clusterSpec = new YarnClusterSpec()
      .setHostType("n1-standard-8")
      .setNetworkPolicy("default-network")
      .setYarnOfferingVersion("2.0")
      .setClusterName(testClusterName)

    // Create a real YarnJob with all required fields
    val yarnJob = new YarnJob()
      .setJobType(YarnJobType.FLINK)
      .setAppName("test-app")
      .setArgsList(List("arg1", "arg2").asJava)
      .setFileWithContents(Map("file1" -> "content1").asJava)
      .setClusterSpec(clusterSpec)

    // Create a real ZiplineJob with the YarnJob set
    val ziplineJob = new ZiplineJob()
    val jobUnion = new JobBase()
    jobUnion.setYarnJob(yarnJob)
    ziplineJob.setJobUnion(jobUnion)

    // Mock cluster existence check and creation
    val mockPagedResponse = mock[ListClustersPagedResponse]
    when(mockClusterClient.listClusters(any())).thenReturn(mockPagedResponse)
    when(mockPagedResponse.iterateAll()).thenReturn(List.empty[Cluster].asJava)
    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setClusterName(testClusterName)
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()
    when(mockClusterClient.createClusterAsync(any[CreateClusterRequest])).thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)
    when(mockClusterClient.getCluster(any(), any(), any())).thenReturn(mockCluster)

    // Mock job submission
    when(
      mockDataprocSubmitter.submit(any[TypeFlinkJob.type],
                                   any[Map[String, String]],
                                   any[Map[String, String]],
                                   any[List[String]],
                                   any[String]))
      .thenReturn(testJobId)

    executor.submitJob(ziplineJob)

    verify(mockDataprocSubmitter).submit(
      any[TypeFlinkJob.type],
      any[Map[String, String]],
      any[Map[String, String]],
      any[List[String]],
      any[String]
    )
  }

  "getJobStatus" should "return correct status" in {
    val executor = createTestExecutor()
    val mockJob = mock[Job]
    val mockStatus = mock[JobStatus]

    when(mockJobClient.getJob(any())).thenReturn(mockJob)
    when(mockJob.getStatus).thenReturn(mockStatus)
    when(mockStatus.getState).thenReturn(JobStatus.State.RUNNING)

    val status = executor.getJobStatus(testJobId)

    status shouldBe JobStatusType.RUNNING
    verify(mockJobClient).getJob(any[GetJobRequest])
  }

  "killJob" should "cancel the job" in {
    val executor = createTestExecutor()

    executor.killJob(testJobId)

    verify(mockJobClient).cancelJob(any[CancelJobRequest])
  }

  "checkForStreamingDataprocCluster" should "return true when cluster exists" in {
    val executor = createTestExecutor()
    val mockCluster = mock[Cluster]

    when(mockCluster.getClusterName).thenReturn(testClusterName)
    val mockPagedResponse = mock[ListClustersPagedResponse]
    when(mockClusterClient.listClusters(any())).thenReturn(mockPagedResponse)
    when(mockPagedResponse.iterateAll()).thenReturn(List(mockCluster).asJava)

    val exists = executor.checkForStreamingDataprocCluster(testClusterName)

    exists shouldBe true
  }

  "checkForStreamingDataprocCluster" should "return false when cluster doesn't exist" in {
    val executor = createTestExecutor()

    val mockPagedResponse = mock[ListClustersPagedResponse]
    when(mockClusterClient.listClusters(any())).thenReturn(mockPagedResponse)
    when(mockPagedResponse.iterateAll()).thenReturn(List.empty[Cluster].asJava)

    val exists = executor.checkForStreamingDataprocCluster("non-existent-cluster")

    exists shouldBe false
  }
}
