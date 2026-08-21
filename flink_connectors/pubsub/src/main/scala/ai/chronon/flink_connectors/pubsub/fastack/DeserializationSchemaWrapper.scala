package ai.chronon.flink_connectors.pubsub.fastack

import ai.chronon.flink.deser.ChrononDeserializationSchema
import ai.chronon.online.serde.RequiresMessageAttribute
import com.google.pubsub.v1.PubsubMessage
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

// Thin wrapper around a Flink DeserializationSchema to allow it to work with PubSub message wrapper objects
class DeserializationSchemaWrapper[T](deserializationSchema: DeserializationSchema[T])
    extends PubSubDeserializationSchema[T] {

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    deserializationSchema.open(context)
  }

  override def isEndOfStream(nextElement: T): Boolean = deserializationSchema.isEndOfStream(nextElement)

  override def deserialize(message: PubsubMessage): T = {
    throw new UnsupportedOperationException(
      "Use the deserialize(message: PubSubMessage, out: Collector[T]) method instead.");
  }

  override def deserialize(message: PubsubMessage, out: Collector[T]): Unit = {
    deserializationSchema.deserialize(DeserializationSchemaWrapper.encodeBytes(message, deserializationSchema), out)
  }

  override def getProducedType: TypeInformation[T] = deserializationSchema.getProducedType
}

object DeserializationSchemaWrapper {

  // Mirrors Confluent's wire-format magic byte (reserved for future version bumps) so the two formats are
  // byte-for-byte comparable at offset 0.
  val MagicByte: Byte = 0x00

  // Only the payload bytes for delegates that don't ask for message attributes - the vast majority of SerDes.
  // Delegates opting in via RequiresMessageAttribute get [1-byte magic][4-byte length][UTF-8 attribute value][payload],
  // with length == -1 signaling the attribute was absent on this particular message.
  private[fastack] def encodeBytes[T](message: PubsubMessage, deserializationSchema: DeserializationSchema[T]): Array[Byte] = {
    val payload = message.getData.toByteArray
    deserializationSchema match {
      case chronon: ChrononDeserializationSchema[_] =>
        chronon.serDe match {
          case attrSerDe: RequiresMessageAttribute =>
            attrSerDe.attributeKey match {
              case Some(key) => encodeWithAttribute(payload, message, key)
              case None      => payload
            }
          case _ => payload
        }
      case _ => payload
    }
  }

  private def encodeWithAttribute(payload: Array[Byte], message: PubsubMessage, attributeKey: String): Array[Byte] = {
    val attrBytes = Option(message.getAttributesMap.get(attributeKey)).map(_.getBytes(StandardCharsets.UTF_8))
    val buf = ByteBuffer.allocate(1 + 4 + attrBytes.map(_.length).getOrElse(0) + payload.length)
    buf.put(MagicByte)
    buf.putInt(attrBytes.map(_.length).getOrElse(-1))
    attrBytes.foreach(buf.put)
    buf.put(payload)
    buf.array()
  }
}
