package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.DummyNode
import ai.chronon.orchestration.temporal.activity.NodeRunnerActivityImpl
import ai.chronon.orchestration.temporal.constants.NodeExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.workflow.{NodeExecutionWorkflow, NodeExecutionWorkflowImpl}
import io.temporal.client.{WorkflowClient, WorkflowClientOptions, WorkflowOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.testing.{TestEnvironmentOptions, TestWorkflowEnvironment}
import io.temporal.worker.Worker
import org.mockito.Mockito.verify
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration

class NodeExecutionWorkflowTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  /** We still go through all the following payload converters in the following order as specified below
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/DefaultDataConverter.java#L38
    * which is important for using other Types serialized using other payload converters, but we will be
    * overriding ByteArrayPayloadConverter with our custom ThriftPayloadConverter based on EncodingType
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/ByteArrayPayloadConverter.java#L30
    */
  private val customDataConverter = DefaultDataConverter.newDefaultInstance.withPayloadConverterOverrides(
    new ThriftPayloadConverter
  )
  private val clientOptions = WorkflowClientOptions
    .newBuilder()
    .setNamespace("test-namespace")
    .setIdentity("test-identity")
    .setDataConverter(customDataConverter)
    .build()
  private val testEnvOptions = TestEnvironmentOptions
    .newBuilder()
    .setWorkflowClientOptions(clientOptions)
    .build()
  private val workflowOptions = WorkflowOptions
    .newBuilder()
    .setTaskQueue(NodeExecutionWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var nodeExecutionWorkflow: NodeExecutionWorkflow = _
  private val mockNodeRunnerActivity: NodeRunnerActivityImpl = mock[NodeRunnerActivityImpl]

  override def beforeEach(): Unit = {
    testEnv = TestWorkflowEnvironment.newInstance(testEnvOptions)
    worker = testEnv.newWorker(NodeExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[NodeExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Register the mock activity with worker
    worker.registerActivitiesImplementations(mockNodeRunnerActivity)

    // Start the test environment
    testEnv.start()

    // Create node execution workflow after starting test environment
    nodeExecutionWorkflow = workflowClient.newWorkflowStub(classOf[NodeExecutionWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "trigger all necessary activities" in {
    // Trigger and wait for the workflow method
    val node = new DummyNode()
    nodeExecutionWorkflow.executeNode(node)

    // Verify both the activity methods are called
    verify(mockNodeRunnerActivity).triggerAndWaitForDependencies(node)
    verify(mockNodeRunnerActivity).submitJob(node)
  }
}
