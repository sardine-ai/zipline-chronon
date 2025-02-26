package ai.chronon.orchestration.temporal.converter

import ai.chronon.api.SerdeUtils
import ai.chronon.api.thrift.TBase
import io.temporal.api.common.v1.Payload
import io.temporal.common.converter.{ByteArrayPayloadConverter, DataConverterException, PayloadConverter}

import java.util.Optional
import java.lang.reflect.Type

class ThriftPayloadConverter extends PayloadConverter {

  private val byteArrayPayloadConverter = new ByteArrayPayloadConverter

  // The encoding type determines which default conversion behavior to override.
  // It's important to use the same encoding type as ByteArrayPayloadConverter and override it
  // otherwise that's first in the order of available payload converters, and we run into serialization errors
  // https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/common/converter/DefaultDataConverter.java#L38
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
