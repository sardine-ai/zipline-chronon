package org.apache.spark.sql.avro

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.Counter
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.util.Try

class AvroDeserializationSchema(topicName: String, jsonSchema: String, schemaRegistryWireFormat: Boolean)
    extends AbstractDeserializationSchema[Row] {

  def encoder: Encoder[Row] = {
    val avroDeserializer = AvroDataToCatalyst(null, jsonSchema, Map.empty)
    val catalystType = avroDeserializer.dataType.asInstanceOf[StructType]
    Encoders.row(catalystType)
  }

  // these are created on instantiation in the various task manager processes in the open() call
  @transient private var avroDeserializer: AvroDataToCatalyst = _
  @transient private var sparkRowDeser: ExpressionEncoder.Deserializer[Row] = _

  @transient private var deserializationErrorCounter: Counter = _
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    val metricsGroup = context.getMetricGroup
      .addGroup("chronon")
      .addGroup("topic", topicName)
    deserializationErrorCounter = metricsGroup.counter("avro_deserialization_errors")
    avroDeserializer = AvroDataToCatalyst(null, jsonSchema, Map.empty)
    sparkRowDeser = encoder.asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()
  }

  override def deserialize(messageBytes: Array[Byte]): Row = {
    def doDeserialize(messageBytes: Array[Byte], errorMessage: String): Try[InternalRow] = {
      Try {
        avroDeserializer.nullSafeEval(messageBytes).asInstanceOf[InternalRow]
      }.recover { case e: Exception =>
        logger.error(errorMessage, e)
        deserializationErrorCounter.inc()
        null
      }
    }

    val maybeMessage = if (schemaRegistryWireFormat) {
      // schema id is set, we skip the first byte and read the schema id based on the wire format:
      // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#messages-wire-format
      val buffer = ByteBuffer.wrap(messageBytes)
      buffer.get
      val messageSchemaId = buffer.getInt

      // unfortunately we need to drop the first 5 bytes (and thus copy the rest of the byte array) as the AvroDataToCatalyst
      // interface takes a byte array and the methods to do the Row conversion etc are all private so we can't reach in
      doDeserialize(messageBytes.drop(5),
                    s"Failed to deserialize message from Avro Bytes to InternalRow. Message schema id $messageSchemaId")
    } else {
      doDeserialize(messageBytes, "Failed to deserialize message from Avro Bytes to InternalRow")
    }

    // return null in case of failure. This allows us to skip the message according to the Flink API
    val deserTry = maybeMessage.map(m => sparkRowDeser(m)).recover { case e: Exception =>
      logger.error("Failed to deserialize InternalRow to Row", e)
      deserializationErrorCounter.inc()
      null
    }

    deserTry.get
  }
}

object AvroDeserializationSupport {
  def build(topicName: String,
            jsonSchema: String,
            schemaRegistryWireFormat: Boolean = false): (Encoder[Row], AvroDeserializationSchema) = {
    val deserializer = new AvroDeserializationSchema(topicName, jsonSchema, schemaRegistryWireFormat)
    (deserializer.encoder, deserializer)
  }
}
