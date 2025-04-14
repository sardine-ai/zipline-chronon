package ai.chronon.orchestration.test.temporal.activity

import ai.chronon.api.{PartitionRange, PartitionSpec}
import ai.chronon.orchestration.NodeRunStatus
import ai.chronon.orchestration.persistence.{NodeDao, NodeRun, NodeTableDependency}
import ai.chronon.orchestration.pubsub.{JobSubmissionMessage, PubSubPublisher}
import ai.chronon.orchestration.temporal.activity.{NodeExecutionActivity, NodeExecutionActivityImpl}
import ai.chronon.orchestration.temporal.constants.NodeSingleStepWorkflowTaskQueue
import ai.chronon.orchestration.temporal.{Branch, NodeExecutionRequest, NodeName, StepDays}
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import ai.chronon.orchestration.test.utils.TemporalTestEnvironmentUtils
import ai.chronon.orchestration.test.utils.TestUtils.createTestTableDependency
import ai.chronon.orchestration.utils.TemporalUtils
import io.temporal.activity.ActivityOptions
import io.temporal.client.{WorkflowClient, WorkflowOptions}
import io.temporal.common.RetryOptions
import io.temporal.testing.{TestActivityEnvironment, TestWorkflowEnvironment}
import io.temporal.worker.Worker
import io.temporal.workflow.{Workflow, WorkflowInterface, WorkflowMethod}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.lang.{Void => JavaVoid}
import java.time.Duration
import java.util.concurrent.CompletableFuture
import scala.concurrent.Future

// Test workflows for activity testing
// These are needed for testing manual completion logic for our activities as it's not supported for
// test activity environment

// Workflow for testing triggerDependency
@WorkflowInterface
trait TestTriggerDependencyWorkflow {
  @WorkflowMethod
  def triggerDependency(nodeExecutionRequest: NodeExecutionRequest): Unit
}

class TestTriggerDependencyWorkflowImpl extends TestTriggerDependencyWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .setRetryOptions(
        RetryOptions
          .newBuilder()
          .setMaximumAttempts(1) // Only try once, no retries
          .build()
      )
      .build()
  )

  override def triggerDependency(nodeExecutionRequest: NodeExecutionRequest): Unit = {
    activity.triggerDependency(nodeExecutionRequest)
  }
}

// Workflow for testing submitJob
@WorkflowInterface
trait TestSubmitJobWorkflow {
  @WorkflowMethod
  def submitJob(nodeName: NodeName): Unit
}

class TestSubmitJobWorkflowImpl extends TestSubmitJobWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .setRetryOptions(
        RetryOptions
          .newBuilder()
          .setMaximumAttempts(1) // Only try once, no retries
          .build()
      )
      .build()
  )

  override def submitJob(nodeName: NodeName): Unit = {
    activity.submitJob(nodeName)
  }
}

// Workflow for testing triggerMissingNodeSteps
@WorkflowInterface
trait TestTriggerMissingNodeStepsWorkflow {
  @WorkflowMethod
  def triggerMissingNodeSteps(nodeName: NodeName, branch: Branch, missingSteps: Seq[PartitionRange]): Unit
}

class TestTriggerMissingNodeStepsWorkflowImpl extends TestTriggerMissingNodeStepsWorkflow {
  private val activity = Workflow.newActivityStub(
    classOf[NodeExecutionActivity],
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(5))
      .setRetryOptions(
        RetryOptions
          .newBuilder()
          .setMaximumAttempts(1) // Only try once, no retries
          .build()
      )
      .build()
  )

  override def triggerMissingNodeSteps(nodeName: NodeName, branch: Branch, missingSteps: Seq[PartitionRange]): Unit = {
    activity.triggerMissingNodeSteps(nodeName, branch, missingSteps)
  }
}

class NodeExecutionActivitySpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with MockitoSugar {

  // Default partition spec used for tests
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  private val testBranch = Branch("test")

  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeSingleStepWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  private var testWorkflowEnv: TestWorkflowEnvironment = _
  private var testActivityEnv: TestActivityEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var mockWorkflowOps: WorkflowOperations = _
  private var mockPublisher: PubSubPublisher = _
  private var mockNodeDao: NodeDao = _
  private var testTriggerWorkflow: TestTriggerDependencyWorkflow = _
  private var testSubmitWorkflow: TestSubmitJobWorkflow = _
  private var testTriggerMissingNodeStepsWorkflow: TestTriggerMissingNodeStepsWorkflow = _
  private var activityImpl: NodeExecutionActivityImpl = _
  private var activity: NodeExecutionActivity = _

  override def beforeEach(): Unit = {
    testWorkflowEnv = TemporalTestEnvironmentUtils.getTestWorkflowEnv
    testActivityEnv = TemporalTestEnvironmentUtils.getTestActivityEnv
    worker = testWorkflowEnv.newWorker(NodeSingleStepWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(
      classOf[TestTriggerDependencyWorkflowImpl],
      classOf[TestSubmitJobWorkflowImpl],
      classOf[TestTriggerMissingNodeStepsWorkflowImpl]
    )
    workflowClient = testWorkflowEnv.getWorkflowClient

    // Get the activity stub (interface) to use for testing with retries disabled
    activity = testActivityEnv.newActivityStub(
      classOf[NodeExecutionActivity],
      ActivityOptions
        .newBuilder()
        .setScheduleToCloseTimeout(Duration.ofSeconds(10))
        .setRetryOptions(
          RetryOptions
            .newBuilder()
            .setMaximumAttempts(1) // Only try once, no retries
            .build()
        )
        .build()
    )

    // Create mock dependencies
    mockWorkflowOps = mock[WorkflowOperations]
    mockPublisher = mock[PubSubPublisher]
    mockNodeDao = mock[NodeDao]
    when(mockPublisher.topicId).thenReturn("test-topic")

    // Create activity with mocked dependencies
    activityImpl = new NodeExecutionActivityImpl(mockWorkflowOps, mockNodeDao, mockPublisher)
    worker.registerActivitiesImplementations(activityImpl)
    testActivityEnv.registerActivitiesImplementations(activityImpl)

    // Start the test environment
    testWorkflowEnv.start()

    // Create test activity workflows
    testTriggerWorkflow = workflowClient.newWorkflowStub(classOf[TestTriggerDependencyWorkflow], workflowOptions)
    testSubmitWorkflow = workflowClient.newWorkflowStub(classOf[TestSubmitJobWorkflow], workflowOptions)
    testTriggerMissingNodeStepsWorkflow =
      workflowClient.newWorkflowStub(classOf[TestTriggerMissingNodeStepsWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    if (testWorkflowEnv != null) {
      testWorkflowEnv.close()
    }
    if (testActivityEnv != null) {
      testActivityEnv.close()
    }
  }

  it should "trigger and successfully wait for activity completion" in {
    val nodeExecutionRequest =
      NodeExecutionRequest(NodeName("test-node"), testBranch, PartitionRange("2023-01-01", "2023-01-02"))
    val completedFuture = CompletableFuture.completedFuture[JavaVoid](null)

    // Mock workflow operations
    when(mockWorkflowOps.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest)).thenReturn(completedFuture)

    // Trigger activity method
    testTriggerWorkflow.triggerDependency(nodeExecutionRequest)

    // Assert
    verify(mockWorkflowOps).startNodeRangeCoordinatorWorkflow(nodeExecutionRequest)
  }

  it should "fail when the dependency workflow fails" in {
    val nodeExecutionRequest =
      NodeExecutionRequest(NodeName("failing-node"), testBranch, PartitionRange("2023-01-01", "2023-01-02"))
    val expectedException = new RuntimeException("Workflow execution failed")
    val failedFuture = new CompletableFuture[JavaVoid]()
    failedFuture.completeExceptionally(expectedException)

    // Mock workflow operations to return a failed future
    when(mockWorkflowOps.startNodeRangeCoordinatorWorkflow(nodeExecutionRequest)).thenReturn(failedFuture)

    // Trigger activity and expect it to fail
    val exception = intercept[RuntimeException] {
      testTriggerWorkflow.triggerDependency(nodeExecutionRequest)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the mocked method was called
    verify(mockWorkflowOps, atLeastOnce()).startNodeRangeCoordinatorWorkflow(nodeExecutionRequest)
  }

  it should "submit job successfully" in {
    val nodeName = NodeName("test-node")
    val completedFuture = CompletableFuture.completedFuture("message-id-123")

    // Mock PubSub publisher to return a completed future
    when(mockPublisher.publish(ArgumentMatchers.any[JobSubmissionMessage])).thenReturn(completedFuture)

    // Trigger activity method
    testSubmitWorkflow.submitJob(nodeName)

    // Use a capture to verify the message passed to the publisher
    val messageCaptor = ArgumentCaptor.forClass(classOf[JobSubmissionMessage])
    verify(mockPublisher).publish(messageCaptor.capture())

    // Verify the message content
    val capturedMessage = messageCaptor.getValue
    capturedMessage.nodeName should be(nodeName.name)
  }

  it should "fail when publishing to PubSub fails" in {
    val nodeName = NodeName("failing-node")
    val expectedException = new RuntimeException("Failed to publish message")
    val failedFuture = new CompletableFuture[String]()
    failedFuture.completeExceptionally(expectedException)

    // Mock PubSub publisher to return a failed future
    when(mockPublisher.publish(ArgumentMatchers.any[JobSubmissionMessage])).thenReturn(failedFuture)

    // Trigger activity and expect it to fail
    val exception = intercept[RuntimeException] {
      testSubmitWorkflow.submitJob(nodeName)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the message was passed to the publisher
    verify(mockPublisher, atLeastOnce()).publish(ArgumentMatchers.any[JobSubmissionMessage])
  }

  it should "get table dependencies correctly" in {
    val nodeName = NodeName("test-node")

    // Create expected NodeTableDependency objects
    val expectedDependencies = Seq(
      NodeTableDependency(
        nodeName,
        NodeName("child1"),
        createTestTableDependency("test_table_1", Some("dt"))
      ),
      NodeTableDependency(
        nodeName,
        NodeName("child2"),
        createTestTableDependency("test_table_2")
      )
    )

    // Mock NodeDao to return table dependencies
    when(mockNodeDao.getNodeTableDependencies(nodeName))
      .thenReturn(Future.successful(expectedDependencies))

    // Get table dependencies
    val dependencies = activity.getTableDependencies(nodeName)

    // Verify the correct number of dependencies are returned
    dependencies.size shouldBe 2

    // Verify that the returned dependencies match the expected ones
    dependencies should contain theSameElementsAs expectedDependencies

    // Verify the mocked method was called
    verify(mockNodeDao).getNodeTableDependencies(nodeName)
  }

  it should "handle errors when getting table dependencies" in {
    val nodeName = NodeName("error-node")

    // Mock NodeDao to return a failed future
    when(mockNodeDao.getNodeTableDependencies(nodeName))
      .thenReturn(Future.failed(new RuntimeException("Database error")))

    // Call the activity and expect an exception
    val exception = intercept[RuntimeException] {
      activity.getTableDependencies(nodeName)
    }

    // Verify the exception message includes the error text
    exception.getMessage should include("Error pulling dependencies")

    // Verify the mock was called
    verify(mockNodeDao).getNodeTableDependencies(nodeName)
  }

  it should "handle an empty list of table dependencies" in {
    val nodeName = NodeName("no-dependencies-node")

    // Mock NodeDao to return an empty list
    when(mockNodeDao.getNodeTableDependencies(nodeName))
      .thenReturn(Future.successful(Seq.empty))

    // Get table dependencies
    val dependencies = activity.getTableDependencies(nodeName)

    // Verify the result is an empty sequence
    dependencies shouldBe empty

    // Verify the mock was called
    verify(mockNodeDao).getNodeTableDependencies(nodeName)
  }

  it should "register node run successfully" in {
    // Create a node run to register
    val nodeRun = NodeRun(
      nodeName = NodeName("test-node"),
      startPartition = "2023-01-01",
      endPartition = "2023-01-31",
      runId = "run-123",
      branch = testBranch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = None,
      status = NodeRunStatus.WAITING
    )

    // Mock NodeDao insertNodeRun
    when(mockNodeDao.insertNodeRun(nodeRun)).thenReturn(Future.successful(1))

    // Call activity method
    activity.registerNodeRun(nodeRun)

    // Verify the mock was called
    verify(mockNodeDao).insertNodeRun(nodeRun)
  }

  it should "update node run status successfully" in {
    // Create a node run to update
    val nodeRun = NodeRun(
      nodeName = NodeName("test-node"),
      startPartition = "2023-01-01",
      endPartition = "2023-01-31",
      runId = "run-123",
      branch = testBranch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Mock NodeDao updateNodeRunStatus
    when(mockNodeDao.updateNodeRunStatus(nodeRun)).thenReturn(Future.successful(1))

    // Call activity method
    activity.updateNodeRunStatus(nodeRun)

    // Verify the mock was called
    verify(mockNodeDao).updateNodeRunStatus(nodeRun)
  }

  // Test suite for getMissingSteps
  "getMissingSteps" should "identify correct missing steps with no existing partitions" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-05")
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq.empty)) // No overlapping runs

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(ai.chronon.orchestration.temporal.StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify the missing partitions (should be Jan 2, Jan 4)
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-01", "2023-01-01"),
      PartitionRange("2023-01-02", "2023-01-02"),
      PartitionRange("2023-01-03", "2023-01-03"),
      PartitionRange("2023-01-04", "2023-01-04"),
      PartitionRange("2023-01-05", "2023-01-05")
    )

    // Verify all partitions are missing (should return the original range)
    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "identify only missing partitions when some partitions already exist" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-05") // 5 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create existing node runs (completed runs for Jan 1 and Jan 3)
    val nodeRun1 = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    val nodeRun2 = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-03",
      endPartition = "2023-01-04",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-03T10:00:00Z",
      endTime = Some("2023-01-03T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(nodeRun1, nodeRun2)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify the missing partitions (should be Jan 5)
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-05", "2023-01-05")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "handle the case where all partitions are already complete" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create existing node runs (completed runs for all days)
    val nodeRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-03", // Covers the whole range
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(nodeRun)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify no missing steps
    missingSteps shouldBe empty

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "ignore failed or incomplete node runs" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create existing node runs with different statuses
    val completedRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-01",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    val failedRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-02",
      endPartition = "2023-01-02",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-02T10:00:00Z",
      endTime = Some("2023-01-02T11:00:00Z"),
      status = NodeRunStatus.FAILED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(completedRun, failedRun)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify the failed partition is identified as missing
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-02", "2023-01-02"),
      PartitionRange("2023-01-03", "2023-01-03")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "handle node runs with different step days correctly" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-05") // 5 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 2 // Two-day step

    // Create existing node runs (completed runs for Jan 1-3)
    val nodeRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-03",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(nodeRun)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // With stepDays=2, we should get one missing range: Jan 4-5
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-04", "2023-01-05")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "prioritize latest node run by start time when multiple runs exist for same partition" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create older failed run
    val olderFailedRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z", // Earlier time
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.FAILED
    )

    // Create newer successful run for same partition
    val newerSuccessfulRun = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-01T12:00:00Z", // Later time
      endTime = Some("2023-01-01T13:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(olderFailedRun, newerSuccessfulRun)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify the newer successful run is used, so Jan 1 partition isn't missing
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-03", "2023-01-03")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "prioritize running jobs over completed jobs regardless of start time" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create older running job (still in progress)
    val runningJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z", // Earlier start time
      endTime = None, // No end time - still running
      status = NodeRunStatus.RUNNING
    )

    // Create newer completed job that finished successfully
    val completedJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-01T12:00:00Z", // Later start time
      endTime = Some("2023-01-01T13:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(runningJob, completedJob)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify we expect all 3 days to be missing
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-01", "2023-01-01"),
      PartitionRange("2023-01-02", "2023-01-02"),
      PartitionRange("2023-01-03", "2023-01-03")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "prioritize most recently started running job when multiple running jobs exist" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create older running job (still in progress)
    val olderRunningJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z", // Earlier start time
      endTime = None, // No end time - still running
      status = NodeRunStatus.RUNNING
    )

    // Create newer running job (also still in progress)
    val newerRunningJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-01T12:00:00Z", // Later start time
      endTime = None, // No end time - still running
      status = NodeRunStatus.RUNNING
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(olderRunningJob, newerRunningJob)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify we expect all 3 days missing
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-01", "2023-01-01"),
      PartitionRange("2023-01-02", "2023-01-02"),
      PartitionRange("2023-01-03", "2023-01-03")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "prioritize most recently completed job when multiple completed jobs exist" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03") // 3 day range
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)
    val stepDays = 1 // Daily step

    // Create job with earlier start time but later end time
    val laterFinishedJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-1",
      branch = branch,
      startTime = "2023-01-01T10:00:00Z", // Earlier start time
      endTime = Some("2023-01-01T15:00:00Z"), // Later end time
      status = NodeRunStatus.SUCCEEDED
    )

    // Create job with later start time but earlier end time
    val earlierFinishedJob = NodeRun(
      nodeName = nodeName,
      startPartition = "2023-01-01",
      endPartition = "2023-01-02",
      runId = "run-2",
      branch = branch,
      startTime = "2023-01-01T12:00:00Z", // Later start time
      endTime = Some("2023-01-01T13:00:00Z"), // Earlier end time
      status = NodeRunStatus.FAILED
    )

    // Setup mocks for finding overlapping node runs
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq(laterFinishedJob, earlierFinishedJob)))

    // Setup mocks for step days
    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.successful(StepDays(stepDays)))

    // Execute the activity
    val missingSteps = activity.getMissingSteps(nodeExecutionRequest)

    // Verify we expect Jan 3 missing, but not Jan 1-2 because they have a successful job
    // The later finished job should be chosen because it has a later end time
    val expectedMissingRanges = Seq(
      PartitionRange("2023-01-03", "2023-01-03")
    )

    missingSteps should contain theSameElementsAs expectedMissingRanges

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "handle exception when fetching overlapping node runs fails" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03")
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)

    // Setup mock to throw exception
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.failed(new RuntimeException("Database connection failed")))

    // Execute the activity and expect exception
    val exception = intercept[RuntimeException] {
      activity.getMissingSteps(nodeExecutionRequest)
    }

    // Verify exception message
    exception.getMessage should include("Error finding overlapping node runs")

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
  }

  it should "handle exception when fetching step days fails" in {
    val nodeName = NodeName("test-node")
    val branch = Branch("test")
    val partitionRange = PartitionRange("2023-01-01", "2023-01-03")
    val nodeExecutionRequest = NodeExecutionRequest(nodeName, branch, partitionRange)

    // Setup mocks
    when(mockNodeDao.findOverlappingNodeRuns(nodeExecutionRequest))
      .thenReturn(Future.successful(Seq.empty))

    when(mockNodeDao.getStepDays(nodeName))
      .thenReturn(Future.failed(new RuntimeException("Step days not found")))

    // Execute the activity and expect exception
    val exception = intercept[RuntimeException] {
      activity.getMissingSteps(nodeExecutionRequest)
    }

    // Verify exception message
    exception.getMessage should include("Error finding step days")

    // Verify mock interactions
    verify(mockNodeDao).findOverlappingNodeRuns(nodeExecutionRequest)
    verify(mockNodeDao).getStepDays(nodeName)
  }

  it should "trigger missing node steps for new runs" in {
    val nodeName = NodeName("test-node")
    val missingSteps = Seq(
      PartitionRange("2023-01-01", "2023-01-02"),
      PartitionRange("2023-01-03", "2023-01-04")
    )

    // Mock findLatestCoveringRun to return None (no existing runs)
    missingSteps.foreach { step =>
      val request = NodeExecutionRequest(nodeName, testBranch, step)
      when(mockNodeDao.findLatestCoveringRun(request))
        .thenReturn(Future.successful(None))
    }

    // Mock startNodeSingleStepWorkflow to return completed futures
    missingSteps.foreach { step =>
      val request = NodeExecutionRequest(nodeName, testBranch, step)
      val completedFuture = CompletableFuture.completedFuture[JavaVoid](null)
      when(mockWorkflowOps.startNodeSingleStepWorkflow(request))
        .thenReturn(completedFuture)
    }

    // Trigger the activity
    testTriggerMissingNodeStepsWorkflow.triggerMissingNodeSteps(nodeName, testBranch, missingSteps)

    // Verify each step was processed
    missingSteps.foreach { step =>
      val request = NodeExecutionRequest(nodeName, testBranch, step)
      verify(mockNodeDao).findLatestCoveringRun(request)
      verify(mockWorkflowOps).startNodeSingleStepWorkflow(request)
    }
  }

  it should "skip already successful runs when triggering missing node steps" in {
    val nodeName = NodeName("test-node")
    val missingSteps = Seq(
      PartitionRange("2023-01-01", "2023-01-02") // One step that's already been successfully run
    )

    // Create a successful node run
    val successfulRun = NodeRun(
      nodeName = nodeName,
      branch = testBranch,
      startPartition = missingSteps.head.start,
      endPartition = missingSteps.head.end,
      runId = "run-123",
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.SUCCEEDED
    )

    // Mock findLatestCoveringRun to return the successful run
    val request = NodeExecutionRequest(nodeName, testBranch, missingSteps.head)
    when(mockNodeDao.findLatestCoveringRun(request))
      .thenReturn(Future.successful(Some(successfulRun)))

    // Trigger the activity
    testTriggerMissingNodeStepsWorkflow.triggerMissingNodeSteps(nodeName, testBranch, missingSteps)

    // Verify findLatestCoveringRun was called
    verify(mockNodeDao).findLatestCoveringRun(request)

    // Verify startNodeSingleStepWorkflow was NOT called (because the run was successful)
    verify(mockWorkflowOps, org.mockito.Mockito.never())
      .startNodeSingleStepWorkflow(request)
  }

  it should "retry failed runs when triggering missing node steps" in {
    val nodeName = NodeName("test-node")
    val missingSteps = Seq(
      PartitionRange("2023-01-01", "2023-01-02") // One step that previously failed
    )

    // Create a failed node run
    val failedRun = NodeRun(
      nodeName = nodeName,
      branch = testBranch,
      startPartition = missingSteps.head.start,
      endPartition = missingSteps.head.end,
      runId = "run-123",
      startTime = "2023-01-01T10:00:00Z",
      endTime = Some("2023-01-01T11:00:00Z"),
      status = NodeRunStatus.FAILED
    )

    // Mock findLatestCoveringRun to return the failed run
    val request = NodeExecutionRequest(nodeName, testBranch, missingSteps.head)
    when(mockNodeDao.findLatestCoveringRun(request))
      .thenReturn(Future.successful(Some(failedRun)))

    // Mock startNodeSingleStepWorkflow to return a completed future
    val completedFuture = CompletableFuture.completedFuture[JavaVoid](null)
    when(mockWorkflowOps.startNodeSingleStepWorkflow(request))
      .thenReturn(completedFuture)

    // Trigger the activity
    testTriggerMissingNodeStepsWorkflow.triggerMissingNodeSteps(nodeName, testBranch, missingSteps)

    // Verify findLatestCoveringRun was called
    verify(mockNodeDao).findLatestCoveringRun(request)

    // Verify startNodeSingleStepWorkflow was called (because the run failed and should be retried)
    verify(mockWorkflowOps).startNodeSingleStepWorkflow(request)
  }

  it should "wait for in-progress runs when triggering missing node steps" in {
    val nodeName = NodeName("test-node")
    val missingSteps = Seq(
      PartitionRange("2023-01-01", "2023-01-02") // One step that's already running
    )

    // Create a running node run
    val runningRun = NodeRun(
      nodeName = nodeName,
      branch = testBranch,
      startPartition = missingSteps.head.start,
      endPartition = missingSteps.head.end,
      runId = "run-123",
      startTime = "2023-01-01T10:00:00Z",
      endTime = None,
      status = NodeRunStatus.RUNNING
    )

    // Mock findLatestCoveringRun to return the running run
    val request = NodeExecutionRequest(nodeName, testBranch, missingSteps.head)
    when(mockNodeDao.findLatestCoveringRun(request))
      .thenReturn(Future.successful(Some(runningRun)))

    // Mock getWorkflowResult to return a completed future
    val workflowId = TemporalUtils.getNodeSingleStepWorkflowId(request)
    val completedFuture = CompletableFuture.completedFuture[JavaVoid](null)
    when(mockWorkflowOps.getWorkflowResult(workflowId, "run-123"))
      .thenReturn(completedFuture)

    // Trigger the activity
    testTriggerMissingNodeStepsWorkflow.triggerMissingNodeSteps(nodeName, testBranch, missingSteps)

    // Verify findLatestCoveringRun was called
    verify(mockNodeDao).findLatestCoveringRun(request)

    // Verify getWorkflowResult was called (to wait for the running workflow)
    verify(mockWorkflowOps).getWorkflowResult(workflowId, "run-123")

    // Verify startNodeSingleStepWorkflow was NOT called (because we're waiting for an existing run)
    verify(mockWorkflowOps, org.mockito.Mockito.never())
      .startNodeSingleStepWorkflow(request)
  }

  it should "fail when a node step workflow fails" in {
    val nodeName = NodeName("failing-node")
    val missingSteps = Seq(
      PartitionRange("2023-01-01", "2023-01-02")
    )

    // Mock findLatestCoveringRun to return None (no existing run)
    val request = NodeExecutionRequest(nodeName, testBranch, missingSteps.head)
    when(mockNodeDao.findLatestCoveringRun(request))
      .thenReturn(Future.successful(None))

    // Mock startNodeSingleStepWorkflow to return a failed future
    val expectedException = new RuntimeException("Workflow execution failed")
    val failedFuture = new CompletableFuture[JavaVoid]()
    failedFuture.completeExceptionally(expectedException)
    when(mockWorkflowOps.startNodeSingleStepWorkflow(request))
      .thenReturn(failedFuture)

    // Trigger activity and expect it to fail
    val exception = intercept[RuntimeException] {
      testTriggerMissingNodeStepsWorkflow.triggerMissingNodeSteps(nodeName, testBranch, missingSteps)
    }

    // Verify that the exception is propagated correctly
    exception.getMessage should include("failed")

    // Verify the mocked methods were called
    verify(mockNodeDao).findLatestCoveringRun(request)
    verify(mockWorkflowOps).startNodeSingleStepWorkflow(request)
  }
}
