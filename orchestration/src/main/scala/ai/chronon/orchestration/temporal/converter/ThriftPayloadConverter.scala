package ai.chronon.orchestration.temporal.converter

import ai.chronon.api.SerdeUtils
import ai.chronon.api.thrift.TBase
import io.temporal.api.common.v1.Payload
import io.temporal.common.converter.{ByteArrayPayloadConverter, DataConverterException, PayloadConverter}

import java.util.Optional
import java.lang.reflect.Type

/** Custom payload converter for Thrift objects in Temporal workflows.
  *
  * This converter enables Temporal to properly serialize and deserialize Thrift objects
  * when they are passed as inputs/outputs to workflow and activity methods. By integrating
  * with Temporals data conversion pipeline, it allows:
  *
  * 1. Passing complex Thrift objects between workflow and activity components
  * 2. Storing Thrift objects in workflow state for continuing execution
  * 3. Persisting Thrift objects in workflow history for replay capabilities
  *
  * The implementation uses the ByteArrayPayloadConverter for the actual binary conversion
  * but adds Thrift-specific serialization/deserialization handling. This ensures type safety
  * and proper evolution of Thrift schemas over time.
  *
  * Note: This converter must override the same encoding type as ByteArrayPayloadConverter
  * to ensure it's used for Thrift objects instead of the default binary converter.
  */
class ThriftPayloadConverter extends PayloadConverter {

  private val byteArrayPayloadConverter = new ByteArrayPayloadConverter

  /** The encoding type determines which default conversion behavior to override.
    * It's important to use the same encoding type as ByteArrayPayloadConverter and override it
    * otherwise that's first in the order of available payload converters, and we run into serialization errors
    * https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/DefaultDataConverter.java#L38
    */
  override def getEncodingType = "binary/plain"

  @throws[DataConverterException]
  override def toData(value: AnyRef): Optional[Payload] = {
    value match {
      case base: TBase[_, _] =>
        try {
          byteArrayPayloadConverter.toData(SerdeUtils.compactSerializer.get().serialize(base))
        } catch {
          case e: DataConverterException =>
            throw e
          case e: Exception =>
            throw new DataConverterException(e)
        }
      case _ =>
        Optional.empty()
    }
  }

  @throws[DataConverterException]
  override def fromData[T](content: Payload, valueClass: Class[T], valueType: Type): T = {
    if (classOf[TBase[_, _]].isAssignableFrom(valueClass)) {
      try {

        val value: T = valueClass.getDeclaredConstructor().newInstance()

        val deser = SerdeUtils.compactDeserializer.get()
        deser.deserialize(value.asInstanceOf[TBase[_, _]], content.getData.toByteArray)
        value

      } catch {
        case e: DataConverterException =>
          throw e
        case e: Exception =>
          throw new DataConverterException(content, valueClass, e)
      }
    } else {
      null.asInstanceOf[T]
    }
  }
}
