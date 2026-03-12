package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission
import ai.chronon.spark.submission.JobSubmitterConstants._
import org.junit.Assert.assertEquals
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model._

import scala.jdk.CollectionConverters._

class EmrServerlessSubmitterTest extends AnyFlatSpec with MockitoSugar {

  private def createSubmitter(
      mockClient: EmrServerlessClient,
      applicationId: Option[String] = Some("app-123"),
      executionRoleArn: String = "arn:aws:iam::123456789012:role/EMRServerlessRole",
      s3LogUri: String = "s3://my-bucket/logs/",
      eksFlinkSubmitter: Option[EksFlinkSubmitter] = None,
      dynamodbTableName: String = "test-table",
      awsRegion: String = "us-east-1",
      eksClusterName: Option[String] = None,
      ingressBaseUrl: Option[String] = None
  ): EmrServerlessSubmitter = {
    new EmrServerlessSubmitter(
      mockClient,
      applicationId,
      executionRoleArn,
      s3LogUri,
      eksFlinkSubmitter = eksFlinkSubmitter,
      dynamodbTableName = dynamodbTableName,
      awsRegion = awsRegion,
      eksClusterName = eksClusterName,
      ingressBaseUrl = ingressBaseUrl
    )
  }

  "EmrServerlessSubmitter" should "submit a Spark job successfully with existing application" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123456"

    val jobRunId = "job-run-123"
    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId(jobRunId)
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id"
      ),
      Map("spark.executor.memory" -> "4g"),
      List.empty,
      Map("team" -> "chronon")
    )

    assertEquals(s"$applicationId|$jobRunId", submittedJobId)
    verify(mockClient).startJobRun(any[StartJobRunRequest])
  }

  it should "create a new application if applicationId is not provided" in {
    val mockClient = mock[EmrServerlessClient]

    val newAppId = "app-new-123"

    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List.empty[ApplicationSummary].asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val createAppResponse = CreateApplicationResponse
      .builder()
      .applicationId(newAppId)
      .build()
    when(mockClient.createApplication(any[CreateApplicationRequest]))
      .thenReturn(createAppResponse)

    val getAppResponse = GetApplicationResponse
      .builder()
      .application(
        Application
          .builder()
          .applicationId(newAppId)
          .state(ApplicationState.CREATED)
          .build()
      )
      .build()
    when(mockClient.getApplication(any[GetApplicationRequest]))
      .thenReturn(getAppResponse)

    val jobRunId = "job-run-123"
    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(newAppId)
      .jobRunId(jobRunId)
      .build()
    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = None)

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id"
      ),
      Map.empty,
      List.empty,
      Map.empty
    )

    assertEquals(s"$newAppId|$jobRunId", submittedJobId)

    verify(mockClient).createApplication(any[CreateApplicationRequest])
    verify(mockClient).startJobRun(any[StartJobRunRequest])
  }

  it should "reuse existing application with matching name" in {
    val mockClient = mock[EmrServerlessClient]

    val existingAppId = "app-existing-123"

    val existingApp = ApplicationSummary
      .builder()
      .id(existingAppId)
      .name("chronon-serverless-app")
      .state(ApplicationState.CREATED)
      .build()
    val listAppsResponse = ListApplicationsResponse
      .builder()
      .applications(List(existingApp).asJava)
      .build()
    when(mockClient.listApplications(any[ListApplicationsRequest]))
      .thenReturn(listAppsResponse)

    val jobRunId = "job-run-456"
    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(existingAppId)
      .jobRunId(jobRunId)
      .build()
    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = None)

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id"
      ),
      Map.empty,
      List.empty,
      Map.empty
    )

    assertEquals(s"$existingAppId|$jobRunId", submittedJobId)

    verify(mockClient, never()).createApplication(any[CreateApplicationRequest])
    verify(mockClient).startJobRun(any[StartJobRunRequest])
  }

  it should "return PENDING status for a submitted job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"
    val jobId = s"$applicationId|$jobRunId"

    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.PENDING)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    assertEquals(JobStatusType.PENDING, submitter.status(jobId))
    verify(mockClient).getJobRun(any[GetJobRunRequest])
  }

  it should "return RUNNING status for a running job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"
    val jobId = s"$applicationId|$jobRunId"

    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.RUNNING)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    assertEquals(JobStatusType.RUNNING, submitter.status(jobId))
  }

  it should "return SUCCEEDED status for a completed job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"
    val jobId = s"$applicationId|$jobRunId"

    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.SUCCESS)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    assertEquals(JobStatusType.SUCCEEDED, submitter.status(jobId))
  }

  it should "return FAILED status for a failed job" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"
    val jobId = s"$applicationId|$jobRunId"

    val getJobRunResponse = GetJobRunResponse
      .builder()
      .jobRun(
        JobRun.builder()
          .applicationId(applicationId)
          .jobRunId(jobRunId)
          .state(JobRunState.FAILED)
          .build()
      )
      .build()
    when(mockClient.getJobRun(any[GetJobRunRequest]))
      .thenReturn(getJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    assertEquals(JobStatusType.FAILED, submitter.status(jobId))
  }

  it should "return UNKNOWN status for invalid jobId format" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)

    assertEquals(JobStatusType.UNKNOWN, submitter.status("invalid-format"))
    verify(mockClient, never()).getJobRun(any[GetJobRunRequest])
  }

  it should "cancel a job successfully" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val jobRunId = "job-run-123"
    val jobId = s"$applicationId|$jobRunId"

    val cancelResponse = CancelJobRunResponse.builder().build()
    when(mockClient.cancelJobRun(any[CancelJobRunRequest]))
      .thenReturn(cancelResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))
    submitter.kill(jobId)

    val requestCaptor = ArgumentCaptor.forClass(classOf[CancelJobRunRequest])
    verify(mockClient).cancelJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    assertEquals(applicationId, capturedRequest.applicationId())
    assertEquals(jobRunId, capturedRequest.jobRunId())
  }

  it should "handle kill gracefully for invalid jobId format" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)

    submitter.kill("invalid-format")

    verify(mockClient, never()).cancelJobRun(any[CancelJobRunRequest])
  }

  it should "delegate Flink status to eksFlinkSubmitter" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[EksFlinkSubmitter]
    when(mockFlinkSubmitter.status("my-deployment", "my-namespace"))
      .thenReturn(JobStatusType.RUNNING)

    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockFlinkSubmitter))

    val status = submitter.status("flink:my-namespace:my-deployment")
    assertEquals(JobStatusType.RUNNING, status)
    verify(mockFlinkSubmitter).status("my-deployment", "my-namespace")
  }

  it should "delegate Flink kill to eksFlinkSubmitter" in {
    val mockClient = mock[EmrServerlessClient]
    val mockFlinkSubmitter = mock[EksFlinkSubmitter]

    val submitter = createSubmitter(mockClient, eksFlinkSubmitter = Some(mockFlinkSubmitter))

    submitter.kill("flink:my-namespace:my-deployment")
    verify(mockFlinkSubmitter).delete("my-deployment", "my-namespace")
  }

  it should "capture StartJobRun request with correct spark submit parameters" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"

    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId("job-run-123")
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId))

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/cloud-aws.jar",
        JobId -> "test-job-id"
      ),
      Map(
        "spark.executor.memory" -> "4g",
        "spark.executor.cores" -> "2"
      ),
      List.empty,
      Map("team" -> "chronon"),
      "--arg1",
      "--arg2=value"
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    assertEquals(applicationId, capturedRequest.applicationId())

    val sparkSubmit = capturedRequest.jobDriver().sparkSubmit()
    assertEquals("s3://my-bucket/jars/cloud-aws.jar", sparkSubmit.entryPoint())

    val args = sparkSubmit.entryPointArguments().asScala
    assert(args.contains("--arg1"))
    assert(args.contains("--arg2=value"))

    assertEquals("chronon", capturedRequest.tags().get("team"))
  }

  it should "include monitoring configuration with S3 log URI" in {
    val mockClient = mock[EmrServerlessClient]
    val applicationId = "app-123"
    val s3LogUri = "s3://my-bucket/emr-logs/"

    val startJobRunResponse = StartJobRunResponse
      .builder()
      .applicationId(applicationId)
      .jobRunId("job-run-123")
      .build()

    when(mockClient.startJobRun(any[StartJobRunRequest]))
      .thenReturn(startJobRunResponse)

    val submitter = createSubmitter(mockClient, applicationId = Some(applicationId), s3LogUri = s3LogUri)

    submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://my-bucket/jars/test.jar",
        JobId -> "test-job"
      ),
      Map.empty,
      List.empty,
      Map.empty
    )

    val requestCaptor = ArgumentCaptor.forClass(classOf[StartJobRunRequest])
    verify(mockClient).startJobRun(requestCaptor.capture())

    val capturedRequest = requestCaptor.getValue
    val monitoringConfig = capturedRequest.configurationOverrides().monitoringConfiguration()
    assertEquals(s3LogUri, monitoringConfig.s3MonitoringConfiguration().logUri())
  }

  it should "return correct jarName" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals("cloud_aws_lib_deploy.jar", submitter.jarName)
  }

  it should "return correct onlineClass" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals("ai.chronon.integrations.aws.AwsApiImpl", submitter.onlineClass)
  }

  it should "return filename from resolveConfPath for Spark --files distribution" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    val s3Uri = "s3://my-bucket/configs/join.json"
    assertEquals("join.json", submitter.resolveConfPath(s3Uri))
  }

  it should "return false for isClusterCreateNeeded" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assertEquals(false, submitter.isClusterCreateNeeded(isLongRunning = true))
    assertEquals(false, submitter.isClusterCreateNeeded(isLongRunning = false))
  }

  it should "return None for ensureClusterReady" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    import scala.concurrent.ExecutionContext.Implicits.global
    assertEquals(None, submitter.ensureClusterReady("cluster", None))
  }

  it should "return correct kvStoreApiProperties" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient, dynamodbTableName = "my-table", awsRegion = "us-west-2")
    val props = submitter.kvStoreApiProperties
    assertEquals("my-table", props("AWS_DYNAMODB_TABLE_NAME"))
    assertEquals("us-west-2", props("AWS_DEFAULT_REGION"))
  }

  it should "return None for getJobUrl for Spark jobs" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient, awsRegion = "us-east-1")
    val url = submitter.getJobUrl("app-123|job-run-456")
    assert(url.isEmpty)
  }

  it should "return Spark UI URL via dashboard API" in {
    val mockClient = mock[EmrServerlessClient]
    val dashboardUrl = "https://es-abc123.emrstudio-prod.us-east-1.amazonaws.com/#/serverless-applications/app-123/job-run-456"
    val response = GetDashboardForJobRunResponse.builder().url(dashboardUrl).build()
    when(mockClient.getDashboardForJobRun(any[GetDashboardForJobRunRequest])).thenReturn(response)
    val submitter = createSubmitter(mockClient, awsRegion = "us-east-1")
    val url = submitter.getSparkUrl("app-123|job-run-456")
    assert(url.isDefined)
    assertEquals(dashboardUrl, url.get)
  }

  it should "return None for getSparkUrl when dashboard API fails" in {
    val mockClient = mock[EmrServerlessClient]
    when(mockClient.getDashboardForJobRun(any[GetDashboardForJobRunRequest]))
      .thenThrow(ValidationException.builder().message("Dashboard not available").build())
    val submitter = createSubmitter(mockClient, awsRegion = "us-east-1")
    val url = submitter.getSparkUrl("app-123|job-run-456")
    assert(url.isEmpty)
  }

  it should "return EKS URL for Flink job" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(
      mockClient,
      awsRegion = "us-east-1",
      eksClusterName = Some("my-cluster")
    )
    val url = submitter.getJobUrl("flink:my-ns:my-deploy")
    assert(url.isDefined)
    assert(url.get.contains("eks/clusters/my-cluster/deployments/my-deploy"))
  }

  it should "return Flink UI URL via ingressBaseUrl" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(
      mockClient,
      ingressBaseUrl = Some("https://hub.example.com")
    )
    val url = submitter.getFlinkUrl("flink:my-ns:my-deploy")
    assert(url.isDefined)
    assertEquals("https://hub.example.com/flink/my-deploy/", url.get)
  }

  it should "return None for getFlinkUrl on non-flink job" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient, ingressBaseUrl = Some("https://hub.example.com"))
    assertEquals(None, submitter.getFlinkUrl("app-123|job-run-456"))
  }

  it should "return empty deprecatedClusterNameEnvVars" in {
    val mockClient = mock[EmrServerlessClient]
    val submitter = createSubmitter(mockClient)
    assert(submitter.deprecatedClusterNameEnvVars.isEmpty)
  }

  /**
    * Integration test that submits a real Spark job to EMR Serverless.
    *
    * This test uses default configuration values that can be overridden with environment variables.
    *
    * Default configuration (modify these or set environment variables):
    * - AWS_REGION: us-east-1
    * - AWS_ACCOUNT_ID: 123456789012 (MUST be set to your actual AWS account)
    * - CUSTOMER_ID: canary (used for role and bucket naming)
    * - EMR_SERVERLESS_APP_ID: (Optional) Pre-created application ID
    * - EMR_EXECUTION_ROLE_ARN: Defaults to arn:aws:iam::{ACCOUNT_ID}:role/zipline_{CUSTOMER_ID}_emr_serverless_role
    * - EMR_LOG_URI: Defaults to s3://zipline-logs-{CUSTOMER_ID}/emr-serverless/
    * - TEST_JAR_URI: Defaults to s3://zipline-artifacts-{CUSTOMER_ID}/jars/cloud-aws.jar
    * - TEST_MAIN_CLASS: ai.chronon.spark.Driver
    * - POLL_JOB_STATUS: false (set to "true" to wait for completion)
    *
    * Example setup:
    * export AWS_ACCOUNT_ID=123456789012
    * export CUSTOMER_ID=canary
    * export POLL_JOB_STATUS=true
    *
    * Then run:
    * ./mill cloud_aws.test.testOnly ai.chronon.integrations.aws.EmrServerlessSubmitterTest -- -n Integration
    */
  it should "submit a real Spark job to EMR Serverless" taggedAs IntegrationTest ignore {
    val awsAccountId = sys.env.getOrElse("AWS_ACCOUNT_ID", "123456789012")
    val customerId = sys.env.getOrElse("CUSTOMER_ID", "canary")
    val region = sys.env.getOrElse("AWS_REGION", "us-east-1")
    val applicationId = sys.env.get("EMR_SERVERLESS_APP_ID")

    val executionRoleArn = sys.env.getOrElse(
      "EMR_EXECUTION_ROLE_ARN",
      s"arn:aws:iam::$awsAccountId:role/zipline_${customerId}_emr_serverless_role"
    )
    val s3LogUri = sys.env.getOrElse(
      "EMR_LOG_URI",
      s"s3://zipline-logs-$customerId/emr-serverless/"
    )
    val jarUri = sys.env.getOrElse(
      "TEST_JAR_URI",
      s"s3://zipline-artifacts-$customerId/jars/cloud-aws.jar"
    )
    val mainClass = sys.env.getOrElse("TEST_MAIN_CLASS", "ai.chronon.spark.Driver")
    val pollStatus = sys.env.getOrElse("POLL_JOB_STATUS", "false").toBoolean

    println(s"=== EMR Serverless Integration Test ===")
    println(s"AWS Account ID: $awsAccountId")
    println(s"Customer ID: $customerId")
    println(s"Region: $region")
    println(s"Application ID: ${applicationId.getOrElse("Will create new")}")
    println(s"Execution Role: $executionRoleArn")
    println(s"Log URI: $s3LogUri")
    println(s"JAR URI: $jarUri")
    println(s"Main Class: $mainClass")
    println(s"Poll Status: $pollStatus")
    println()

    val client = EmrServerlessClient.builder()
      .region(Region.of(region))
      .build()

    val submitter = new EmrServerlessSubmitter(
      client,
      applicationId,
      executionRoleArn,
      s3LogUri
    )

    val jobId = s"chronon-test-${System.currentTimeMillis()}"

    println(s"Submitting job with ID: $jobId")

    val submittedJobId = submitter.submit(
      submission.SparkJob,
      Map(
        MainClass -> mainClass,
        JarURI -> jarUri,
        JobId -> jobId
      ),
      Map(
        "spark.executor.cores" -> "2",
        "spark.executor.memory" -> "4g",
        "spark.driver.cores" -> "1",
        "spark.driver.memory" -> "2g"
      ),
      List.empty,
      Map(
        "test" -> "integration",
        "framework" -> "chronon"
      ),
      "--help"
    )

    println(s"Job submitted successfully!")
    println(s"Job ID: $submittedJobId")

    val parts = submittedJobId.split("\\|")
    val appId = parts(0)
    val runId = parts(1)

    println(s"Application ID: $appId")
    println(s"Job Run ID: $runId")
    println(
      s"Console URL: https://console.aws.amazon.com/emr/home?region=$region#/serverless-applications/$appId/job-runs/$runId")
    println()

    assert(submittedJobId.contains("|"), "Job ID should be in format: applicationId|jobRunId")

    if (pollStatus) {
      println("Polling job status...")
      var currentStatus = submitter.status(submittedJobId)
      var attempts = 0
      val maxAttempts = 60

      while (attempts < maxAttempts && (currentStatus == JobStatusType.PENDING || currentStatus == JobStatusType.RUNNING)) {
        println(s"Attempt ${attempts + 1}/$maxAttempts: Status = $currentStatus")
        Thread.sleep(10000)
        attempts += 1
        currentStatus = submitter.status(submittedJobId)
      }

      println(s"Final Status: $currentStatus (after $attempts attempts)")

      if (attempts >= maxAttempts) {
        println(s"Timeout: Job still in $currentStatus state after ${maxAttempts * 10} seconds")
      } else {
        assert(
          currentStatus == JobStatusType.SUCCEEDED || currentStatus == JobStatusType.FAILED,
          s"Job should reach terminal state, but got: $currentStatus"
        )
      }
    } else {
      println("Skipping status polling (set POLL_JOB_STATUS=true to enable)")
      println("You can check the job status manually at the console URL above")
    }
  }
}

object IntegrationTest extends org.scalatest.Tag("Integration")
