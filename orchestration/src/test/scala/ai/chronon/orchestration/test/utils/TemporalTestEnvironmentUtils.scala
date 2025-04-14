package ai.chronon.orchestration.test.utils

import ai.chronon.orchestration.temporal.converter.ThriftPayloadConverter
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.temporal.client.{WorkflowClient, WorkflowClientOptions}
import io.temporal.common.converter.{DefaultDataConverter, JacksonJsonPayloadConverter}
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.testing.{TestActivityEnvironment, TestEnvironmentOptions, TestWorkflowEnvironment}

object TemporalTestEnvironmentUtils {

  // Create a custom ObjectMapper with Scala module
  private val objectMapper = JacksonJsonPayloadConverter.newDefaultObjectMapper
  objectMapper.registerModule(new DefaultScalaModule)
  // Configure ObjectMapper to ignore unknown properties during deserialization
  objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  // Create a custom JacksonJsonPayloadConverter with the Scala-aware ObjectMapper
  private val scalaJsonConverter = new JacksonJsonPayloadConverter(objectMapper)

  /** We still go through all the following payload converters in the following order as specified below
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/DefaultDataConverter.java#L38
    * which is important for using other Types serialized using other payload converters, but we will be
    * overriding ByteArrayPayloadConverter with our custom ThriftPayloadConverter based on EncodingType
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/ByteArrayPayloadConverter.java#L30
    */
  private val customDataConverter = DefaultDataConverter.newDefaultInstance.withPayloadConverterOverrides(
    new ThriftPayloadConverter,
    scalaJsonConverter
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
