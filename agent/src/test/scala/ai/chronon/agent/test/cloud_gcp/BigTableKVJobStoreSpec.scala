package ai.chronon.agent.test.cloud_gcp

import agent.src.test.scala.ai.chronon.agent.test.utils.TestUtils
import ai.chronon.agent.{JobStore, KVJobStore}
import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_gcp.BigTableKVStoreImpl
import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.bigtable.admin.v2.{BigtableTableAdminClient, BigtableTableAdminSettings}
import com.google.cloud.bigtable.data.v2.{BigtableDataClient, BigtableDataSettings}
import com.google.cloud.bigtable.emulator.v2.Emulator
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmulatorWrapper {
  private var emulator: Emulator = null

  /** Initializes the Bigtable emulator before a test runs. */
  def before(): Unit = {
    emulator = Emulator.createBundled
    emulator.start()
  }

  /** Stops the Bigtable emulator after a test finishes. */
  def after(): Unit = {
    emulator.stop()
    emulator = null
  }

  def getPort: Int = emulator.getPort
}

class BigTableKVJobStoreSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val emulatorWrapper = new EmulatorWrapper

  // BigTable clients
  private var dataClient: BigtableDataClient = _
  private var adminClient: BigtableTableAdminClient = _

  private var jobStore: JobStore = _

  private val projectId = "test-project"
  private val instanceId = "test-instance"

  before {
    emulatorWrapper.before()

    // Configure settings to use emulator
    val dataSettings = BigtableDataSettings
      .newBuilderForEmulator(emulatorWrapper.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    val adminSettings = BigtableTableAdminSettings
      .newBuilderForEmulator(emulatorWrapper.getPort)
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()

    // Create clients
    dataClient = BigtableDataClient.create(dataSettings)
    adminClient = BigtableTableAdminClient.create(adminSettings)

    // Create KVStore with the emulator client
    val kvStore = new BigTableKVStoreImpl(dataClient, Some(adminClient))

    // Create JobStore
    jobStore = new KVJobStore(kvStore)
  }

  after {
    // Close bigtable clients
    if (dataClient != null) {
      dataClient.close()
    }
    if (adminClient != null) {
      adminClient.close()
    }

    emulatorWrapper.after()
  }

  "BigTableKVJobStore" should "store and retrieve jobs" in {
    // Create a test job
    val jobId = s"test-job-${System.currentTimeMillis()}"
    val job = TestUtils.createTestJob(jobId)

    // Store the job
    jobStore.storeJob(jobId, job)

    // Retrieve the job
    val retrievedJob = jobStore.getJob(jobId)

    // Verify
    retrievedJob shouldBe defined
    retrievedJob.foreach { j =>
      j.getJobInfo.getJobId shouldBe jobId
      j.getJobInfo.getCurrentStatus shouldBe JobStatusType.PENDING
    }
  }

  it should "update job status" in {
    // Create a test job
    val jobId = s"test-job-status-${System.currentTimeMillis()}"
    val job = TestUtils.createTestJob(jobId)

    // Store the job
    jobStore.storeJob(jobId, job)

    // Update status using updateJobStatus method
    jobStore.updateJobStatus(jobId, JobStatusType.RUNNING)

    // Retrieve the job
    val retrievedJob = jobStore.getJob(jobId)

    // Verify
    retrievedJob shouldBe defined
    retrievedJob.foreach { j =>
      j.getJobInfo.getJobId shouldBe jobId
      j.getJobInfo.getCurrentStatus shouldBe JobStatusType.RUNNING
    }
  }

  it should "retrieve all active jobs" in {
    // Create active and completed jobs
    val activeJob1Id = s"test-active-1-${System.currentTimeMillis()}"
    val activeJob2Id = s"test-active-2-${System.currentTimeMillis()}"
    val completedJobId = s"test-completed-${System.currentTimeMillis()}"

    val activeJob1 = TestUtils.createTestJob(activeJob1Id)
    val activeJob2 = TestUtils.createTestJob(activeJob2Id)
    val completedJob = TestUtils.createTestJob(completedJobId)

    // Make the completed job actually completed
    completedJob.getJobInfo.setCurrentStatus(JobStatusType.SUCCEEDED)

    // Store jobs
    jobStore.storeJob(activeJob1Id, activeJob1)
    jobStore.storeJob(activeJob2Id, activeJob2)
    jobStore.storeJob(completedJobId, completedJob)

    // List active jobs
    val activeJobs = jobStore.getAllActiveJobs

    // Verify
    activeJobs.map(_.getJobInfo.getJobId).toSet should contain allOf (activeJob1Id, activeJob2Id)
    activeJobs.map(_.getJobInfo.getJobId).toSet should not contain completedJobId
  }

  it should "move jobs from active to completed datasets when status changes" in {
    // Create a test job that starts as active (PENDING)
    val jobId = s"test-transition-${System.currentTimeMillis()}"
    val job = TestUtils.createTestJob(jobId)

    // Store job (should go to active dataset)
    jobStore.storeJob(jobId, job)

    // Verify it's in active jobs
    val activeJobs1 = jobStore.getAllActiveJobs
    activeJobs1.map(_.getJobInfo.getJobId).toSet should contain(jobId)

    // Update to completed status
    jobStore.updateJobStatus(jobId, JobStatusType.SUCCEEDED)

    // Verify it's no longer in active jobs
    val activeJobs2 = jobStore.getAllActiveJobs
    activeJobs2.map(_.getJobInfo.getJobId).toSet should not contain jobId

    // But we can still retrieve it directly
    val retrievedJob = jobStore.getJob(jobId)
    retrievedJob shouldBe defined
    retrievedJob.foreach { j =>
      j.getJobInfo.getJobId shouldBe jobId
      j.getJobInfo.getCurrentStatus shouldBe JobStatusType.SUCCEEDED
    }
  }

  it should "move jobs from completed to active datasets when status changes" in {
    // Create a test job that starts as completed (FAILED)
    val jobId = s"test-transition-${System.currentTimeMillis()}"
    val job = TestUtils.createTestJob(jobId)
    job.getJobInfo.setCurrentStatus(JobStatusType.FAILED)

    // Store job (should go to completed dataset)
    jobStore.storeJob(jobId, job)

    // Verify it's not in active jobs
    val activeJobs1 = jobStore.getAllActiveJobs
    activeJobs1.map(_.getJobInfo.getJobId).toSet should not contain (jobId)

    // Update to pending status
    jobStore.updateJobStatus(jobId, JobStatusType.PENDING)

    // Verify it's moved to active jobs
    val activeJobs2 = jobStore.getAllActiveJobs
    activeJobs2.map(_.getJobInfo.getJobId).toSet should contain(jobId)

    // Verify it's in pending status
    val retrievedJob = jobStore.getJob(jobId)
    retrievedJob shouldBe defined
    retrievedJob.foreach { j =>
      j.getJobInfo.getJobId shouldBe jobId
      j.getJobInfo.getCurrentStatus shouldBe JobStatusType.PENDING
    }
  }
}
