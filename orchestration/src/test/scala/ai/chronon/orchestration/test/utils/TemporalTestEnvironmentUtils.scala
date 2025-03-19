package ai.chronon.orchestration.test.utils

import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import io.temporal.client.{WorkflowClient, WorkflowClientOptions}
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.testing.{TestActivityEnvironment, TestEnvironmentOptions, TestWorkflowEnvironment}

object TemporalTestEnvironmentUtils {

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
    .setDataConverter(customDataConverter)
    .build()
  private val testEnvOptions = TestEnvironmentOptions
    .newBuilder()
    .setWorkflowClientOptions(clientOptions)
    .build()

  def getTestWorkflowEnv: TestWorkflowEnvironment = {
    TestWorkflowEnvironment.newInstance(testEnvOptions)
  }

  def getTestActivityEnv: TestActivityEnvironment = {
    TestActivityEnvironment.newInstance(testEnvOptions)
  }

  def getLocalWorkflowClient: WorkflowClient = {
    // Initialize workflow client from local service stub
    val serviceStub = WorkflowServiceStubs.newLocalServiceStubs
    WorkflowClient.newInstance(serviceStub, clientOptions)
  }
}
