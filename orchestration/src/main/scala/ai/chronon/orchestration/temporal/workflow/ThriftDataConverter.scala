package ai.chronon.orchestration.temporal.workflow

import io.temporal.api.common.v1.Payload
import io.temporal.api.common.v1.Payloads
import ai.chronon.api.SerdeUtils
import ai.chronon.api.thrift.TBase
import io.temporal.common.converter.ByteArrayPayloadConverter
import io.temporal.common.converter.{DataConverter, DataConverterException}
import java.lang.reflect.Type
import java.util._

class ThriftDataConverter extends DataConverter {

  private val byteArrayPayloadConverter = new ByteArrayPayloadConverter

  @throws[DataConverterException]
  override def toPayload[T](value: T): Optional[Payload] = {
    try {
      require(value.isInstanceOf[TBase[_, _]], "Passed in object is not a thrift object.")
      byteArrayPayloadConverter.toData(SerdeUtils.compactSerializer.get().serialize(value.asInstanceOf[TBase[_, _]]))
    } catch {
      case e: DataConverterException =>
        throw e
      case e: Exception =>
        throw new DataConverterException(e)
    }
  }

  @throws[DataConverterException]
  override def fromPayload[T](payload: Payload, valueClass: Class[T], valueType: Type): T = try {

    val value: T = valueClass.getDeclaredConstructor().newInstance()

    val deser = SerdeUtils.compactDeserializer.get()
    deser.deserialize(value.asInstanceOf[TBase[_, _]], payload.getData.toByteArray)
    value

  } catch {
    case e: DataConverterException =>
      throw e
    case e: Exception =>
      throw new DataConverterException(payload, valueClass, e)
  }

  @throws[DataConverterException]
  def toPayloads(values: AnyRef*): Optional[Payloads] = {
    if (values == null || values.isEmpty) return Optional.empty[Payloads]

    try {

      val result = Payloads.newBuilder
      for (value <- values) { result.addPayloads(toPayload(value).get) }
      Optional.of(result.build)

    } catch {

      case e: DataConverterException =>
        throw e

      case e: Throwable =>
        throw new DataConverterException(e)

    }
  }

  @throws[DataConverterException]
  override def fromPayloads[T](index: Int,
                               content: Optional[Payloads],
                               parameterType: Class[T],
                               genericParameterType: Type): T = {
    if (!content.isPresent) return null.asInstanceOf[T]
    val count = content.get.getPayloadsCount
    if (index >= count) return null.asInstanceOf[T]
    fromPayload(content.get.getPayloads(index), parameterType, genericParameterType)
  }
}
