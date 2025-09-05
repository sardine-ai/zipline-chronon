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
import com.google.cloud.storage.Storage
import com.google.protobuf.Empty
import org.junit.Assert.assertEquals
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class DataprocSubmitterTest extends AnyFlatSpec with MockitoSugar {

  it should "test buildFlinkJob with the expected flinkStateUri and savepointUri" in {
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          gcsClient = mock[GCSClient],
                                          region = "test-region",
                                          projectId = "test-project")
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

    assertEquals(flinkJob.getJarFileUrisList.size(), 21)
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
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
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
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
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
    expectedJarFileUris.foreach(jarUri => jarFileUris.contains(jarUri))
  }

  it should "test createSubmissionPropsMap for spark job" in {

    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val mockControllerClient = mock[JobControllerClient]
    val submitter =
      DataprocSubmitter(jobControllerClient = mockControllerClient,
                        storageClient = mock[Storage],
                        region = "test-region",
                        projectId = "test-project")
    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.SparkJob,
      envMap = Map.empty,
      args = Array(
        s"$JarUriArgKeyword=gs://zipline-jars/cloud-gcp.jar",
        s"$MainClassKeyword=ai.chronon.spark.Driver",
        s"$LocalConfPathArgKeyword=${path.toAbsolutePath.toString}",
        s"$ConfTypeArgKeyword=group_bys",
        s"$OriginalModeArgKeyword=backfill",
        s"$ZiplineVersionArgKeyword=0.1.0",
        s"$JobIdArgKeyword=job-id"
      ),
      clusterName = "test-cluster"
    )

    assertEquals(actual(ZiplineVersion), "0.1.0")
    assertEquals(actual(MetadataName), "quickstart.purchases.v1")
    assertEquals(actual(MainClass), "ai.chronon.spark.Driver")
    assertEquals(actual(JarURI), "gs://zipline-jars/cloud-gcp.jar")
    assert(actual.contains(JobId))
  }

  it should "test createSubmissionPropsMap for flink job with latest savepoint" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val latestFlinkCheckpoint = "gs://zl-warehouse/flink-state/1234/chk-12"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val submitterSpy = org.mockito.Mockito.spy[DataprocSubmitter](submitter)
    when(
      submitterSpy.getLatestFlinkCheckpoint(
        groupByName = groupByName,
        manifestBucketPath = manifestBucketPath,
        flinkCheckpointUri = flinkCheckpointUri
      )).thenReturn(Some(latestFlinkCheckpoint))

    val actual = submitterSpy.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
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
      ),
      clusterName = "test-cluster"
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
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
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
      ),
      clusterName = "test-cluster"
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
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

    val manifestBucketPath = "gs://zipline-warehouse/flink-manifest"
    val groupByName = "quickstart.purchases.v1"
    val flinkCheckpointUri = "gs://zl-warehouse/flink-state/checkpoints"
    val ziplineVersion = "0.1.0"
    val mainClass = "ai.chronon.flink.FlinkJob"
    val jarURI = "gs://zipline-jars/cloud-gcp.jar"
    val flinkMainJarURI = "gs://zipline-jars/flink-assembly-0.1.0-SNAPSHOT.jar"

    val userPassedSavepoint = "gs://zl-warehouse/flink-state/1234/chk-12"

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
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
      ),
      clusterName = "test-cluster"
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
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

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

    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val actual = submitter.createSubmissionPropsMap(
      jobType = submission.FlinkJob,
      envMap = Map.empty,
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
      ),
      clusterName = "test-cluster"
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
        "--arg1=value1"
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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw MoreThanOneRunningFlinkJob("Multiple running Flink jobs found")
    )

    assertThrows[MoreThanOneRunningFlinkJob](
      submitter.run(args = args, clusterName = "test-cluster"))
  }
  it should "fail flink check-if-job-is-running if no running flink job" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw NoRunningFlinkJob("No running Flink job found")
    )

    assertThrows[NoRunningFlinkJob](
      submitter.run(args = args, clusterName = "test-cluster"))
  }
  it should "return early flink check-if-job-is-running if one flink job found" in {
    val groupByName = "test-groupby-name"
    val args = Array(
      s"$JobTypeArgKeyword=flink",
      s"$GroupByNameArgKeyword=$groupByName",
      s"$StreamingModeArgKeyword=$CheckIfJobIsRunning"
    )
    val submitter = mock[DataprocSubmitter]

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("job-id-1")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "job-id-1")
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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("job-id-1")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "job-id-1")
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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw new Exception("No savepoint deploy strategy provided")
    )

    val error =
      intercept[Exception](submitter.run(args = args, clusterName = "test-cluster"))
    assertEquals(error.getMessage, "No savepoint deploy strategy provided")
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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenAnswer(_ =>
      throw new Exception("Multiple savepoint deploy strategies provided")
    )

    val error =
      intercept[Exception](submitter.run(args = args, clusterName = "test-cluster"))
    assert(error.getMessage contains "Multiple savepoint deploy strategies provided")
  }

  it should "test flink deploy with no-savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
  }

  it should "test flink deploy with latest savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
  }

  it should "test flink deploy with user provided savepoint deploy strategy successfully" in {
    val path = Paths.get(getClass.getClassLoader.getResource("group_bys/team/purchases.v1").getPath)

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

    when(submitter.run(args = args, clusterName = "test-cluster")).thenReturn("new-job-id")

    val jobId = submitter.run(args = args, clusterName = "test-cluster")
    assertEquals(jobId, "new-job-id")
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
      DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                        storageClient = mock[Storage],
                        region = "test-region",
                        projectId = "test-project")

    val submittedJobId =
      submitter.submit(
        spark.submission.SparkJob,
        Map(
          MainClass -> "test-main-class",
          JarURI -> "test-jar-uri",
          MetadataName -> "metadata-name",
          ZiplineVersion -> "some-zipline-version",
          JobId -> jobId,
          ClusterName -> "test-cluster"
        ),
        Map.empty,
        List.empty,
        Map.empty
      )
    assertEquals(submittedJobId, jobId)
  }

  it should "test formatDataprocLabel successfully for lowercase" in {
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val label = submitter.formatDataprocLabel("TEST")
    assertEquals(label, "test")
  }

  it should "test formatDataprocLabel successfully and keep lowercase, numbers, underscores, and dashes" in {
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
    val label = submitter.formatDataprocLabel("test-123_5")
    assertEquals(label, "test-123_5")
  }

  it should "test formatDataprocLabel successfully and replace invalid characters with underscore" in {
    val submitter = DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")
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
    val submitter = DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

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

  it should "recreate the cluster if it is in a bad state" in {
    val mockDataprocClient = mock[ClusterControllerClient]

    val mockErrorCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.ERROR))
      .build()
    val mockRunningCluster = Cluster
      .newBuilder()
      .setStatus(ClusterStatus.newBuilder().setState(ClusterStatus.State.RUNNING))
      .build()

    when(mockDataprocClient.getCluster(any[String], any[String], any[String]))
      .thenReturn(mockErrorCluster)
      .thenReturn(mockRunningCluster)

    val mockDeleteOperationFuture = mock[OperationFuture[Empty, ClusterOperationMetadata]]
    val mockDeleteRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockDeleteMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]

    when(mockDataprocClient.deleteClusterAsync(any[DeleteClusterRequest]))
      .thenReturn(mockDeleteOperationFuture)
    when(mockDeleteOperationFuture.getPollingFuture).thenReturn(mockDeleteRetryingFuture)
    when(mockDeleteOperationFuture.peekMetadata()).thenReturn(mockDeleteMetadataFuture)

    val mockOperationFuture = mock[OperationFuture[Cluster, ClusterOperationMetadata]]
    val mockRetryingFuture = mock[RetryingFuture[OperationSnapshot]]
    val mockMetadataFuture = mock[ApiFuture[ClusterOperationMetadata]]

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)
    when(mockOperationFuture.getPollingFuture).thenReturn(mockRetryingFuture)
    when(mockOperationFuture.peekMetadata()).thenReturn(mockMetadataFuture)
    when(mockOperationFuture.get(anyLong(), any[TimeUnit])).thenReturn(mockRunningCluster)

    when(mockDataprocClient.createClusterAsync(any[CreateClusterRequest]))
      .thenReturn(mockOperationFuture)

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
    verify(mockDataprocClient).createClusterAsync(any())
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
    val submitter = DataprocSubmitter(jobControllerClient = mockJobControllerClient,
                                      storageClient = mock[Storage],
                                      region = "test-region",
                                      projectId = "test-project")

    val result = submitter.getZiplineVersionOfDataprocJob(jobId)
    assertEquals(result, expectedVersion)
  }

  it should "test getLatestFlinkCheckpoint should return None if no manifest file found" in {
    val ziplineGcsClient = mock[GCSClient]
    val submitter = new DataprocSubmitter(jobControllerClient = mock[JobControllerClient],
                                          region = "test-region",
                                          projectId = "test-project",
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
                                          region = "test-region",
                                          projectId = "test-project",
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
                                          region = "test-region",
                                          projectId = "test-project",
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
                                          region = "test-region",
                                          projectId = "test-project",
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

    val region = "us-central1"
    val projectId = "canary-443022"
    val clusterName = "zipline-canary-cluster"
    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val submitter = DataprocSubmitter(jobControllerClient, mock[Storage], region, projectId)
    val submittedJobId =
      submitter.submit(
        spark.submission.FlinkJob,
        Map(
          MainClass -> "ai.chronon.flink.FlinkKafkaItemEventDriver",
          MetadataName -> "test-item-event-data",
          ZiplineVersion -> "0.1.0",
          FlinkMainJarURI -> "gs://zipline-jars/flink_kafka_ingest-assembly-0.1.0-SNAPSHOT.jar",
          JarURI -> "gs://zipline-jars/cloud_gcp_bigtable.jar",
          JobId -> ("kafka-driver-" + UUID.randomUUID().toString),
          // This is where we write out checkpoints / persist state while the job is running
          FlinkCheckpointUri -> "gs://zl-warehouse/flink-state",
          ClusterName -> clusterName
        ),
        Map.empty,
        List.empty,
        Map.empty,
        "--kafka-bootstrap=bootstrap.zipline-kafka-cluster.us-central1.managedkafka.canary-443022.cloud.goog:9092",
        "--kafka-topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }

  it should "test flink PubSub ingest job locally" ignore {

    val region = "us-central1"
    val projectId = "canary-443022"
    val clusterName = "zipline-canary-cluster"
    val endPoint = s"${region}-dataproc.googleapis.com:443"
    val jobControllerClient =
      JobControllerClient.create(JobControllerSettings.newBuilder().setEndpoint(endPoint).build())
    val submitter = DataprocSubmitter(jobControllerClient, mock[Storage], region, projectId)
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
          FlinkCheckpointUri -> "gs://zl-warehouse/flink-state",
          ClusterName -> clusterName
        ),
        Map.empty,
        List.empty,
        Map.empty,
        "--gcp-project=canary-443022",
        "--topic=test-item-event-data",
        "--data-file-name=gs://zl-warehouse/canary_item_events/events-output.avro",
        "--event-delay-millis=10"
      )
    println(submittedJobId)
  }
}
