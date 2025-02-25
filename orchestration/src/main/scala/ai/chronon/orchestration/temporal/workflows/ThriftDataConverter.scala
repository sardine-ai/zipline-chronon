package ai.chronon.orchestration.temporal.workflows

//import java.nio.charset.StandardCharsets.UTF_8
//import com.google.common.base.Defaults
//import com.google.common.base.Preconditions
import io.temporal.api.common.v1.Payload
import io.temporal.api.common.v1.Payloads
//import io.temporal.api.failure.v1.Failure
import io.temporal.common.converter.{DataConverter, DataConverterException}
//import io.temporal.failure.DefaultFailureConverter
//import io.temporal.failure.TemporalFailure
//import io.temporal.payload.context.SerializationContext
import java.lang.reflect.Type
import java.util._
//import javax.annotation.Nonnull
//import javax.annotation.Nullable

class ThriftDataConverter extends DataConverter {

  @throws[DataConverterException]
  override def toPayload[T](value: T): Optional[Nothing] = {
    import scala.collection.JavaConversions._
    for (converter <- converters) {
      val result = (if (serializationContext != null) converter.withContext(serializationContext)
                    else converter).toData(value)
      if (result.isPresent) return result
    }
    throw new DataConverterException(
      "No PayloadConverter is registered with this DataConverter that accepts value:" + value)
  }

  @throws[DataConverterException]
  def fromPayload[T](payload: Nothing, valueClass: Class[T], valueType: Nothing): T = try {
    val encoding = payload.getMetadataOrThrow(EncodingKeys.METADATA_ENCODING_KEY).toString(UTF_8)
    val converter = convertersMap.get(encoding)
    if (converter == null)
      throw new DataConverterException("No PayloadConverter is registered for an encoding: " + encoding)
    (if (serializationContext != null) converter.withContext(serializationContext)
     else converter).fromData(payload, valueClass, valueType)
  } catch {
    case e: DataConverterException =>
      throw e
    case e: Exception =>
      throw new DataConverterException(payload, valueClass, e)
  }

  @throws[DataConverterException]
  def toPayloads(values: AnyRef*): Optional[Nothing] = {
    if (values == null || values.length == 0) return Optional.empty
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
  def fromPayloads[T](index: Int,
                      content: Optional[Nothing],
                      parameterType: Class[T],
                      genericParameterType: Nothing): T = {
    if (!content.isPresent) return Defaults.defaultValue(parameterType)
    val count = content.get.getPayloadsCount
    // To make adding arguments a backwards compatible change
    if (index >= count) return Defaults.defaultValue(parameterType)
    fromPayload(content.get.getPayloads(index), parameterType, genericParameterType)
  }
}
