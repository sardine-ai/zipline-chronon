package org.apache.spark.sql.avro

import ai.chronon.api.{DataType, GroupBy}
import ai.chronon.flink.{ChrononDeserializationSchema, SourceProjection, SparkExpressionEval}
import ai.chronon.online.SparkConversions
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.util.Try

abstract class BaseAvroDeserializationSchema[T](groupBy: GroupBy, jsonSchema: String, schemaRegistryWireFormat: Boolean)
    extends ChrononDeserializationSchema[T] {
  // these are created on instantiation in the various task manager processes in the open() call
  @transient private var avroDeserializer: AvroDataToCatalyst = _

  @transient protected var deserializationErrorCounter: Counter = _
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def sourceEventEncoder: Encoder[Row] = AvroCatalystUtils.buildEncoder(jsonSchema)

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    val metricsGroup = context.getMetricGroup
      .addGroup("chronon")
      .addGroup("group_by", groupBy.getMetaData.getName)
    deserializationErrorCounter = metricsGroup.counter("avro_deserialization_errors")
    avroDeserializer = AvroCatalystUtils.buildAvroDataToCatalyst(jsonSchema)
  }

  protected def avroToInternalRow(messageBytes: Array[Byte]): Try[InternalRow] = {
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

    maybeMessage
      .recover { case e: Exception =>
        logger.error("Failed to deserialize InternalRow to Row", e)
        deserializationErrorCounter.inc()
        null
      }
  }
}

class AvroSourceIdentityDeserializationSchema(groupBy: GroupBy, jsonSchema: String, schemaRegistryWireFormat: Boolean)
    extends BaseAvroDeserializationSchema[Row](groupBy, jsonSchema, schemaRegistryWireFormat) {

  override def sourceProjectionEnabled: Boolean = false

  @transient private var sparkRowDeser: ExpressionEncoder.Deserializer[Row] = _

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    sparkRowDeser = sourceEventEncoder.asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()
  }

  override def deserialize(messageBytes: Array[Byte]): Row = {

    val maybeMessage = avroToInternalRow(messageBytes)
    // return null in case of failure. This allows us to skip the message according to the Flink API
    val deserTry = maybeMessage.map(m => sparkRowDeser(m)).recover { case e: Exception =>
      logger.error("Failed to deserialize InternalRow to Row", e)
      deserializationErrorCounter.inc()
      null
    }

    deserTry.get
  }
}

class AvroSourceProjectionDeserializationSchema(groupBy: GroupBy, jsonSchema: String, schemaRegistryWireFormat: Boolean)
    extends BaseAvroDeserializationSchema[Map[String, Any]](groupBy, jsonSchema, schemaRegistryWireFormat)
    with SourceProjection {

  @transient private var evaluator: SparkExpressionEval[Row] = _
  @transient private var rowSerializer: ExpressionEncoder.Serializer[Row] = _
  @transient protected var performSqlErrorCounter: Counter = _

  override def sourceProjectionEnabled: Boolean = true

  override def projectedSchema: Array[(String, DataType)] = {
    val evaluator = new SparkExpressionEval[Row](sourceEventEncoder, groupBy)

    evaluator.getOutputSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }
  }

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    val metricsGroup = context.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)

    performSqlErrorCounter = metricsGroup.counter("avro_deserialization_errors")

    // spark expr eval vars
    val eventExprEncoder = sourceEventEncoder.asInstanceOf[ExpressionEncoder[Row]]
    rowSerializer = eventExprEncoder.createSerializer()
    evaluator = new SparkExpressionEval[Row](sourceEventEncoder, groupBy)
    evaluator.initialize(metricsGroup)
  }

  override def deserialize(messageBytes: Array[Byte], out: Collector[Map[String, Any]]): Unit = {
    val maybeMessage = avroToInternalRow(messageBytes)
    maybeMessage.foreach(row => doSparkExprEval(row, out))
  }

  override def deserialize(messageBytes: Array[Byte]): Map[String, Any] = {
    throw new UnsupportedOperationException(
      "Use the deserialize(message: Array[Byte], out: Collector[Map[String, Any]]) method instead.")
  }

  private def doSparkExprEval(inputEvent: InternalRow, out: Collector[Map[String, Any]]): Unit = {
    try {
      val maybeRow = evaluator.performSql(inputEvent)
      maybeRow.foreach(out.collect)

    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error("Error evaluating Spark expression", e)
        performSqlErrorCounter.inc()
    }
  }
}
