package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark
import ai.chronon.spark.submission
import ai.chronon.spark.submission.JobSubmitterConstants._
import com.google.api.core.ApiFuture
import com.google.api.gax.longrunning.{OperationFuture, OperationSnapshot}
import com.google.api.gax.retrying.RetryingFuture
import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.dataproc.v1.JobControllerClient.ListJobsPagedResponse
import com.google.cloud.dataproc.v1._
import com.google.cloud.dataproc.v1.stub.JobControllerStub
import org.junit.Assert.assertEquals
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.UUID
import scala.jdk.CollectionConverters._

class DataprocSubmitterTest extends AnyFlatSpec with MockitoSugar {
  def setEnv(key: String, value: String): Unit = {
    val env = System.getenv()
    val field = env.getClass.getDeclaredField("m")
    field.setAccessible(true)
    val writableEnv = field.get(env).asInstanceOf[java.util.Map[String, String]]
    writableEnv.put(key, value)
  }

  it should "test buildFlinkJob with the expected flinkStateUri and savepointUri" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = Option("gs://zipline-warehouse/flink-state/groupby-name/chk-1"),
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )

    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)

    val flinkJob = job.getFlinkJob
//    TODO: getMainClass returns empty in tests but not in prod
//    assert(flinkJob.getMainClass == "ai.chronon.flink.FlinkJob")

    assertEquals(flinkJob.getJarFileUrisList.size(), 1)
    assertEquals(flinkJob.getJarFileUrisList.get(0), "gs://zipline-jars/cloud-gcp.jar")

    assertEquals(flinkJob.getMainJarFileUri, "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar")

    assertEquals(flinkJob.getArgsList.size(), 2)
    assertEquals(flinkJob.getArgsList.get(0), "args1")
    assertEquals(flinkJob.getArgsList.get(1), "args2")

    assertEquals(flinkJob.getSavepointUri, "gs://zipline-warehouse/flink-state/groupby-name/chk-1")

    assertEquals(
      flinkJob.getPropertiesMap.asScala,
      Map(
        "metrics.reporters" -> "prom",
        "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
        "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
        "taskmanager.memory.network.max" -> "2G",
        "taskmanager.numberOfTaskSlots" -> "4",
        "yarn.classpath.include-user-jar" -> "FIRST",
        "state.backend.incremental" -> "true",
        "key" -> "value",
        "taskmanager.memory.task.off-heap.size" -> "1G",
        "state.backend.type" -> "rocksdb",
        "taskmanager.memory.managed.fraction" -> "0.5f",
        "state.savepoints.dir" -> "gs://zl-warehouse/flink-state",
        "taskmanager.memory.network.min" -> "1G",
        "metrics.reporter.prom.host" -> "localhost",
        "taskmanager.memory.jvm-metaspace.size" -> "512m",
        "metrics.reporter.prom.port" -> "9250-9260",
        "metrics.reporter.statsd.interval" -> "60 SECONDS",
        "taskmanager.memory.process.size" -> "64G",
        "state.checkpoint-storage" -> "filesystem",
        "state.checkpoints.dir" -> "gs://zl-warehouse/flink-state",
        "state.checkpoints.num-retained" -> "10",
        "jobmanager.memory.process.size" -> "4G",
        "rest.flamegraph.enabled" -> "true"
      )
    )

  }
  it should "test buildFlinkJob with no savepointUri" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )
    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)
    val flinkJob = job.getFlinkJob
    assert(flinkJob.getSavepointUri.isEmpty)
  }

  it should "test buildFlinkJob with pubsub connector uri" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val job = submitter.buildFlinkJob(
      mainClass = "ai.chronon.flink.FlinkJob",
      jarUris = Array("gs://zipline-jars/cloud-gcp.jar", "gs://zipline-jars/flink-pubsub-connector.jar"),
      mainJarUri = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar",
      flinkCheckpointUri = "gs://zl-warehouse/flink-state",
      maybeSavePointUri = None,
      jobProperties = Map("key" -> "value"),
      args = List("args1", "args2"): _*
    )
    assertEquals(job.getTypeJobCase, Job.TypeJobCase.FLINK_JOB)
    val flinkJob = job.getFlinkJob
    val jarFileUris = flinkJob.getJarFileUrisList.asScala.toSet
    val expectedJarFileUris = Set(
      "gs://zipline-jars/cloud-gcp.jar",
      "gs://zipline-jars/flink-pubsub-connector.jar"
    )
    assertEquals(jarFileUris, expectedJarFileUris)
  }

  it should "test createSubmissionPropsMap for spark job" in {

    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val mockControllerClient = mock[JobControllerClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mockControllerClient,
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val actual = DataprocSubmitter.createSubmissionPropsMap(
      jobType = submission.SparkJob,
      submitter = submitter,
      args = Array(
        s"$JarUriArgKeyword=gs://zipline-jars/cloud-gcp.jar",
        s"$MainClassKeyword=ai.chronon.spark.Driver",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=backfill",
        s"$ZiplineVersionArgKeyword=0.1.0",
        s"$JobIdArgKeyword=job-id"
      )
    )

    assertEquals(actual(ZiplineVersion), "0.1.0")
    assertEquals(actual(MetadataName), "quickstart.purchases.v1")
    assertEquals(actual(MainClass), "ai.chronon.spark.Driver")
    assertEquals(actual(JarURI), "gs://zipline-jars/cloud-gcp.jar")
    assert(actual.contains(JobId))
  }

  it should "test createSubmissionPropsMap for flink job with latest savepoint" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val latestFlinkCheckpoint = "gs://zl-warehouse/flink-state/1234/chk-12"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = mock[DataprocSubmitter]
    when(
      submitter.getLatestFlinkCheckpoint(
        groupByName = groupByName,
        manifestBucketPath = manifestBucketPath,
        flinkCheckpointUri = flinkCheckpointUri
      )).thenReturn(Some(latestFlinkCheckpoint))

    val actual = DataprocSubmitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      submitter = submitter,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingLatestSavepointArgKeyword",
        s"$JobIdArgKeyword=job-id"
      )
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)
    assertEquals(actual(SavepointUri), latestFlinkCheckpoint)
  }
  it should "test createSubmissionPropsMap for flink job with no savepoint" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = mock[DataprocSubmitter]

    val actual = DataprocSubmitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      submitter = submitter,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$StreamingNoSavepointArgKeyword",
        s"$JobIdArgKeyword=job-id"
      )
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)

    assert(!actual.contains(SavepointUri))
  }
  it should "test createSubmissionPropsMap for flink job with user passed savepoint" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val userPassedSavepoint = "gs://zl-warehouse/flink-state/1234/chk-12"

    val submitter = mock[DataprocSubmitter]

    val actual = DataprocSubmitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      submitter = submitter,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCustomSavepointArgKeyword=$userPassedSavepoint",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$JobIdArgKeyword=job-id"
      )
    )

    assertEquals(actual(ZiplineVersion), ziplineVersion)
    assertEquals(actual(MetadataName), groupByName)
    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(FlinkCheckpointUri), flinkCheckpointUri)

    assertEquals(actual(SavepointUri), userPassedSavepoint)
  }

  it should "test createSubmissionPropsMap for flink job with additional jars" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val additionalJars = "gs://zipline-jars/some-jar.jar,gs://zipline-jars/another-jar.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val pubSubConnectorJarURI = "gs://zipline-jars/flink-pubsub-connector.jar"
    val userPassedSavepoint = "gs://zl-warehouse/flink-state/1234/chk-12"

    val submitter = mock[DataprocSubmitter]

    val actual = DataprocSubmitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      submitter = submitter,
      args = Array(
        s"$JarUriArgKeyword=$jarURI",
        s"$AdditionalJarsUriArgKeyword=$additionalJars",
        s"$MainClassKeyword=$mainClass",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=streaming",
        s"$ZiplineVersionArgKeyword=$ziplineVersion",
        s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
        s"$FlinkPubSubJarUriArgKeyword=$pubSubConnectorJarURI",
        s"$GroupByNameArgKeyword=$groupByName",
        s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
        s"$StreamingCustomSavepointArgKeyword=$userPassedSavepoint",
        s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
        s"$JobIdArgKeyword=job-id"
      )
    )

    assertEquals(actual(MainClass), mainClass)
    assertEquals(actual(JarURI), jarURI)
    assertEquals(actual(FlinkMainJarURI), flinkMainJarURI)
    assertEquals(actual(AdditionalJars), additionalJars)
    assertEquals(actual(FlinkPubSubConnectorJarURI), pubSubConnectorJarURI)
  }

  it should "test getDataprocFilesArgs when empty" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs()
    assert(actual.isEmpty)
  }

  it should "test getDataprocFilesArgs when one file" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs(
      args = Array(
        s"$FilesArgKeyword=gs://zipline-warehouse/metadata/file.txt"
      )
    )
    assertEquals(actual, List("gs://zipline-warehouse/metadata/file.txt"))
  }

  it should "test getDataprocFilesArgs when more than one file" in {
    val actual = DataprocSubmitter.getDataprocFilesArgs(
      args = Array(
        s"$FilesArgKeyword=gs://zipline-warehouse/metadata/file.txt,gs://zipline-warehouse/metadata/file2.txt"
      )
    )
    assertEquals(actual, List("gs://zipline-warehouse/metadata/file.txt", "gs://zipline-warehouse/metadata/file2.txt"))
  }

  it should "test getApplicationArgs filtering out args for spark job" in {
    val gcpBigtableInstanceId = "test-instance-id"
    val projectId = "test-project-id"

    val envMap = Map(
      GcpBigtableInstanceIdEnvVar -> Option("test-instance-id"),
      GcpProjectIdEnvVar -> Option("test-project-id")
    )

    val internalArgsToFilter = SharedInternalArgs
      .map(k => s"$k=dummy_value")
      .toArray

    val argsToKeep = Array("--arg1=value1")

    val inputArgs = internalArgsToFilter ++ argsToKeep

    val actual = DataprocSubmitter.getApplicationArgs(jobType = submission.SparkJob, envMap = envMap, args = inputArgs)

    assert(
      actual sameElements Array(
        "--arg1=value1",
        "--is-gcp",
        s"--gcp-project-id=${projectId}",
        s"--gcp-bigtable-instance-id=$gcpBigtableInstanceId"
      ))
  }
  it should "test getApplicationArgs filtering out args for flink job" in {
    val internalArgsToFilter = SharedInternalArgs
      .map(k => s"$k=dummy_value")
      .toArray ++ Array(s"$ConfTypeArgKeyword=group_bys")

    val argsToKeep = Array("--arg1=value1")
    val actual = DataprocSubmitter.getApplicationArgs(jobType = submission.FlinkJob,
                                                      envMap = Map.empty,
                                                      args = internalArgsToFilter ++ argsToKeep)
    assert(actual sameElements argsToKeep)
  }

  it should "test run flink job should fail if more than one running flink job found" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1", "job-id-2")
    )

    assertThrows[MoreThanOneRunningFlinkJob](DataprocSubmitter.run(args = args, submitter = submitter))
  }
  it should "fail flink check-if-job-is-running if no running flink job" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List.empty
    )

    assertThrows[NoRunningFlinkJob](DataprocSubmitter.run(args = args, submitter = submitter))
  }
  it should "return early flink check-if-job-is-running if one flink job found" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )

    DataprocSubmitter.run(args = args, submitter = submitter)

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
  }
  it should "return early flink deploy if version check deploy is on and zipline versions match" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingVersionCheckDeploy",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )

    val formattedZiplineVersion = "0_1_0"
    when(submitter.formatDataprocLabel(localZiplineVersion)).thenReturn(
      formattedZiplineVersion
    )
    when(submitter.getZiplineVersionOfDataprocJob("job-id-1")).thenReturn(
      formattedZiplineVersion
    )

    DataprocSubmitter.run(args = args, submitter = submitter)

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
    verify(submitter).formatDataprocLabel(localZiplineVersion)
    verify(submitter).getZiplineVersionOfDataprocJob("job-id-1")
  }

  it should "fail flink deploy if no savepoint deploy strategy provided" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )

    val error = intercept[Exception](DataprocSubmitter.run(args = args, submitter = submitter))
    assertEquals(error.getMessage, "No savepoint deploy strategy provided")

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
  }

  it should "fail flink deploy if multiple savepoint deploy strategies provided" in {
    val groupByName = "test-groupby-name"
    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$StreamingNoSavepointArgKeyword",
      s"$StreamingLatestSavepointArgKeyword"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )

    val error = intercept[Exception](DataprocSubmitter.run(args = args, submitter = submitter))
    assert(error.getMessage contains "Multiple savepoint deploy strategies provided")

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
  }

  it should "test flink deploy with no-savepoint deploy strategy successfully" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=gs://zl-warehouse/flink-state/checkpoints",
      s"$StreamingNoSavepointArgKeyword",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )
    when(submitter.getConf).thenReturn(SubmitterConf("test-project", "test-region", "test-cluster"))

    DataprocSubmitter.run(args = args, submitter = submitter)

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
    verify(submitter).kill("job-id-1")
    verify(submitter).submit(
      jobType = any(),
      submissionProperties = any(),
      jobProperties = any(),
      files = any(),
      any()
    )
    verify(submitter, times(2)).getConf
  }

  it should "test flink deploy with latest savepoint deploy strategy successfully" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
      s"$StreamingLatestSavepointArgKeyword",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )
    when(submitter.getConf).thenReturn(SubmitterConf("test-project", "test-region", "test-cluster"))
    when(
      submitter.getLatestFlinkCheckpoint(
        groupByName = groupByName,
        manifestBucketPath = manifestBucketPath,
        flinkCheckpointUri = flinkCheckpointUri
      )).thenReturn(Some("gs://zl-warehouse/flink-state/1234/chk-12"))

    DataprocSubmitter.run(args = args, submitter = submitter)

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
    verify(submitter).kill("job-id-1")
    verify(submitter).submit(
      jobType = any(),
      submissionProperties = any(),
      jobProperties = any(),
      files = any(),
      any()
    )
    verify(submitter).getLatestFlinkCheckpoint(groupByName = groupByName,
                                               manifestBucketPath = manifestBucketPath,
                                               flinkCheckpointUri = flinkCheckpointUri)
    verify(submitter, times(2)).getConf
  }

  it should "test flink deploy with user provided savepoint deploy strategy successfully" in {
    val confPath = "chronon/cloud_gcp/src/test/resources/group_bys/team/purchases.v1"
    val runfilesDir = Option(System.getenv("RUNFILES_DIR")).getOrElse(".")
    val path = Paths.get(runfilesDir, confPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"

    val localZiplineVersion = "0.1.0"

    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$StreamingModeArgKeyword=$StreamingDeploy",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$LocalZiplineVersionArgKeyword=$localZiplineVersion",
      s"$JarUriArgKeyword=$jarURI",
      s"$MainClassKeyword=$mainClass",
      s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
      s"$ConfTypeArgKeyword=group_bys",
      s"$OriginalModeArgKeyword=streaming",
      s"$ZiplineVersionArgKeyword=$ziplineVersion",
      s"$FlinkMainJarUriArgKeyword=$flinkMainJarURI",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingManifestPathArgKeyword=$manifestBucketPath",
      s"$StreamingCheckpointPathArgKeyword=$flinkCheckpointUri",
      s"$StreamingCustomSavepointArgKeyword=gs://zl-warehouse/flink-state/checkpoints/1234/chk-12",
      s"$JobIdArgKeyword=job-id"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.listRunningGroupByFlinkJobs(groupByName = groupByName)).thenReturn(
      List("job-id-1")
    )
    when(submitter.getConf).thenReturn(SubmitterConf("test-project", "test-region", "test-cluster"))

    DataprocSubmitter.run(args = args, submitter = submitter)

    verify(submitter).listRunningGroupByFlinkJobs(groupByName = groupByName)
    verify(submitter).kill("job-id-1")
    verify(submitter).submit(
      jobType = any(),
      submissionProperties = any(),
      jobProperties = any(),
      files = any(),
      any()
    )
    verify(submitter, times(2)).getConf
  }

  it should "test spark job run successfully" in {

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
      new DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                            conf = SubmitterConf("test-project", "test-region", "test-cluster"))

    val submittedJobId =
      submitter.submit(
        spark.submission.SparkJob,
        Map(MainClass -> "test-main-class",
            JarURI -> "test-jar-uri",
            MetadataName -> "metadata-name",
            ZiplineVersion -> "some-zipline-version",
            JobId -> jobId),
        Map.empty,
        List.empty
      )
    assertEquals(submittedJobId, jobId)
  }

  it should "test formatDataprocLabel successfully for lowercase" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val label = submitter.formatDataprocLabel("TEST")
    assertEquals(label, "test")
  }

  it should "test formatDataprocLabel successfully and keep lowercase, numbers, underscores, and dashes" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val label = submitter.formatDataprocLabel("test-123_5")
    assertEquals(label, "test-123_5")
  }

  it should "test formatDataprocLabel successfully and replace invalid characters with underscore" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))
    val label = submitter.formatDataprocLabel("team.groupby_test")
    assertEquals(label, "team_groupby_test")
  }

  it should "test listRunningGroupByFlinkJobs successfully" in {
    val jobId = "mock-job-id"
    val mockJob = Job
      .newBuilder()
      .setReference(JobReference.newBuilder().setJobId(jobId))
      .setStatus(JobStatus.newBuilder().setState(JobStatus.State.RUNNING))
      .build()

    val mockListJobsPagedResponse = mock[ListJobsPagedResponse]
    val mockJobControllerClient = mock[JobControllerClient]
    val listJobsRequestCapture = ArgumentCaptor.forClass(classOf[ListJobsRequest])
    when(mockJobControllerClient.listJobs(listJobsRequestCapture.capture()))
      .thenReturn(mockListJobsPagedResponse)
    when(mockListJobsPagedResponse.iterateAll()).thenReturn(
      java.util.Collections.singletonList(mockJob)
    )
    val submitter = new DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))

    // Test starts here.
    val runningJobs =
      submitter.listRunningGroupByFlinkJobs("test-groupby-name")

    assertEquals(runningJobs.size, 1)
    assertEquals(runningJobs.head, jobId)

    val listRequest = listJobsRequestCapture.getValue

    assertEquals(listRequest.getProjectId, "test-project")
    assertEquals(listRequest.getRegion, "test-region")
    assertEquals(listRequest.getFilter,
                 "status.state = ACTIVE AND labels.job-type = flink AND labels.metadata-name = test-groupby-name")

  }

  it should "create a Dataproc cluster successfully with a given config" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

    when(mockDataprocClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)

    val region = "test-region"
    val projectId = "test-project"

    val clusterConfigStr = """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      },
      "workerConfig": {
        "numInstances": 2,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val clusterName =
      DataprocSubmitter.getOrCreateCluster("",
                                           Option(Map("dataproc.config" -> clusterConfigStr)),
                                           projectId,
                                           region,
                                           mockDataprocClient)

    assert(clusterName.startsWith("zipline-"))
    verify(mockDataprocClient).createClusterAsync(any())
  }

  it should "not create a new cluster if given name exists" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]
    val mockCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

    when(mockDataprocClient.getCluster(any[String], any[String], any[String])).thenReturn(mockCluster)

    val region = "test-region"
    val projectId = "test-project"

    val clusterConfigStr = """{
      "masterConfig": {
        "numInstances": 1,
        "machineTypeUri": "n1-standard-4"
      },
      "workerConfig": {
        "numInstances": 2,
        "machineTypeUri": "n1-standard-4"
      }
    }"""

    val clusterName =
      DataprocSubmitter.getOrCreateCluster("test-cluster",
                                           Option(Map("dataproc.config" -> clusterConfigStr)),
                                           projectId,
                                           region,
                                           mockDataprocClient)

    assert(clusterName.equals("test-cluster"))
    verify(mockDataprocClient, never()).createClusterAsync(any())
  }

  it should "test getZiplineVersionOfDataprocJob successfully" in {
    val jobId = "mock-job-id"
    val mockJob = mock[Job]
    val expectedVersion = "0_1_0"
    when(mockJob.getLabelsMap).thenReturn(
      java.util.Collections.singletonMap("zipline-version", expectedVersion)
    )

    val mockJobControllerClient = mock[JobControllerClient]
    when(mockJobControllerClient.getJob("test-project", "test-region", jobId)).thenReturn(mockJob)
    val submitter = new DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"))

    val result = submitter.getZiplineVersionOfDataprocJob(jobId)
    assertEquals(result, expectedVersion)
  }

  it should "test getLatestFlinkCheckpoint should return None if no manifest file found" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"),
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(false)

    assert(
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
        .isEmpty
    )
  }

  it should "test getLatestFlinkCheckpoint should throw exception if flink job id not found in manifest" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"),
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"parentJobId=$expectedParentJobId".getBytes)

    assertThrows[RuntimeException] {
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
    }
  }

  it should "test getLatestFlinkCheckpoint should return None if no checkpoints are found in GCS" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"),
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedFlinkJobId = "some-flink-job-id"
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"flinkJobId=$expectedFlinkJobId,parentJobId=$expectedParentJobId".getBytes())

    when(ziplineGcsClient.listFiles(s"gs://test-bucket/flink-state/$expectedFlinkJobId"))
      .thenReturn(Iterator[String]())
    assert(
      submitter
        .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                  manifestBucketPath = "gs://test-bucket/flink-manifest",
                                  flinkCheckpointUri = "gs://test-bucket/flink-state")
        .isEmpty
    )
  }

  it should "test getLatestFlinkCheckpoint should return the latest checkpoint if found" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          conf = SubmitterConf("test-project", "test-region", "test-cluster"),
                                          gcsClient = ziplineGcsClient)

    when(ziplineGcsClient.fileExists("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(true)
    val expectedFlinkJobId = "some-flink-job-id"
    val expectedParentJobId = "some-parent-job-id"
    when(ziplineGcsClient.downloadObjectToMemory("gs://test-bucket/flink-manifest/test-groupby-name/manifest.txt"))
      .thenReturn(s"flinkJobId=$expectedFlinkJobId,parentJobId=$expectedParentJobId".getBytes())

    when(ziplineGcsClient.listFiles(s"gs://test-bucket/flink-state/$expectedFlinkJobId"))
      .thenReturn(
        List(
          "gs://test-bucket/flink-state/some-flink-job-id/chk-9",
          "gs://test-bucket/flink-state/some-flink-job-id/chk-11",
          "gs://test-bucket/flink-state/some-flink-job-id/chk-10"
        ).iterator
      )

    val actual = submitter
      .getLatestFlinkCheckpoint(groupByName = "test-groupby-name",
                                manifestBucketPath = "gs://test-bucket/flink-manifest",
                                flinkCheckpointUri = "gs://test-bucket/flink-state")
    assert(
      actual.isDefined
    )
    assertEquals(actual.get, "gs://test-bucket/flink-state/some-flink-job-id/chk-11")
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
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }

  it should "test flink PubSub ingest job locally" ignore {

    val submitterConf = SubmitterConf("canary-443022", "us-central1", "zipline-canary-cluster")
    val submitter = DataprocSubmitter(submitterConf)
    val submittedJobId =
      submitter.submit(
        spark.submission.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink_connectors.pubsub.FlinkPubSubItemEventDriver",
          MetadataName -> "test-item-event-data",
          ZiplineVersion -> "0.1.0",
          FlinkMainJarURI -> "gs://zipline-jars/flink_pubsub_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JobId -> ("pubsub-driver-" + UUID.randomUUID().toString),
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
          // This is where we write out checkpoints / persist state while the job is running
          FlinkCheckpointUri -> "gs://zl-warehouse/flink-state"
        ),
        Map.empty,
        List.empty,
        "--gcp-project=canary-443022",
        "--topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }
}
