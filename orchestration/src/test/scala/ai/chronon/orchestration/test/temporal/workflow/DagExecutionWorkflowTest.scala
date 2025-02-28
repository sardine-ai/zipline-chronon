package ai.chronon.orchestration.test.temporal.workflow

import ai.chronon.orchestration.{DagExecutionWorkflowState, DummyNodeGraph}
import ai.chronon.orchestration.temporal.constants.DagExecutionWorkflowTaskQueue
import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import ai.chronon.orchestration.temporal.workflow.{DagExecutionWorkflow, DagExecutionWorkflowImpl}
import ai.chronon.orchestration.utils.FuncUtils
import io.temporal.client.{WorkflowClient, WorkflowClientOptions, WorkflowOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.testing.{TestEnvironmentOptions, TestWorkflowEnvironment}
import io.temporal.worker.Worker
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util

class DagExecutionWorkflowTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

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
    .setTaskQueue(DagExecutionWorkflowTaskQueue.toString)
    .setWorkflowExecutionTimeout(Duration.ofSeconds(3))
    .build()
  private lazy val dagExecutionWorkflowState = new DagExecutionWorkflowState()
    .setProcessedDagCount(0L)
    .setMaxHistoryLength(100)
    .setPendingDags(new util.ArrayList[DummyNodeGraph]())

  private var testEnv: TestWorkflowEnvironment = _
  private var worker: Worker = _
  private var workflowClient: WorkflowClient = _
  private var dagExecutionWorkflow: DagExecutionWorkflow = _

  override def beforeEach(): Unit = {
    testEnv = TestWorkflowEnvironment.newInstance(testEnvOptions)
    worker = testEnv.newWorker(DagExecutionWorkflowTaskQueue.toString)
    worker.registerWorkflowImplementationTypes(classOf[DagExecutionWorkflowImpl])
    workflowClient = testEnv.getWorkflowClient

    // Start the test environment
    testEnv.start()

    // Create dag execution workflow after starting test environment
    dagExecutionWorkflow = workflowClient.newWorkflowStub(classOf[DagExecutionWorkflow], workflowOptions)
  }

  override def afterEach(): Unit = {
    testEnv.close()
  }

  it should "execute single DAG" in {
    // Start the workflow asynchronously
    WorkflowClient.start(FuncUtils.toTemporalProc(dagExecutionWorkflow.run(dagExecutionWorkflowState)))

    // Signal the workflow for dag execution
    dagExecutionWorkflow.executeDag(new DummyNodeGraph())

    // Fast-forward time in test environment to simulate workflow completion
    // This executes everything in realtime and only advances internal clock of test environment
    testEnv.sleep(Duration.ofSeconds(1))

    dagExecutionWorkflow.getProcessedDagCount should be(1)
  }

  it should "execute multiple DAGs" in {
    // Start the workflow asynchronously
    WorkflowClient.start(FuncUtils.toTemporalProc(dagExecutionWorkflow.run(dagExecutionWorkflowState)))

    // Signal multiple DAGs
    dagExecutionWorkflow.executeDag(new DummyNodeGraph())
    testEnv.sleep(Duration.ofSeconds(1))

    dagExecutionWorkflow.executeDag(new DummyNodeGraph())
    testEnv.sleep(Duration.ofSeconds(1))

    dagExecutionWorkflow.getProcessedDagCount should be(2)
  }

  it should "continue as new when history length exceeds limit by maintaining intermediate state" in {
    val newStateWithLowerHistoryLimit = dagExecutionWorkflowState.deepCopy().setMaxHistoryLength(5)
    WorkflowClient.start(FuncUtils.toTemporalProc(dagExecutionWorkflow.run(newStateWithLowerHistoryLimit)))

    // Execute multiple DAGs to build up history
    val dags = (1 to 10).map(_ => new DummyNodeGraph())

    dags.foreach { dag =>
      dagExecutionWorkflow.executeDag(dag)
      testEnv.sleep(Duration.ofSeconds(1))
    }

    // Maintains state properly even after the workflow "ContinueAsNew" operation
    dagExecutionWorkflow.getProcessedDagCount should be(10)
  }
}
