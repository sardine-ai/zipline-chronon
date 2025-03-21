package org.apache.spark.sql.avro

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.ScalaJavaConversions.{ListOps, MapOps}
import ai.chronon.api.{Constants, DataType, GroupBy, Query, StructType => ChrononStructType}
import ai.chronon.flink.ChrononDeserializationSchema
import ai.chronon.online.{CatalystUtil, SparkConversions}
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.util.Collector
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.util.Try

class ProjectedAvroDeserializationSchema(groupBy: GroupBy, jsonSchema: String, schemaRegistryWireFormat: Boolean)
    extends ChrononDeserializationSchema {

  // these are created on instantiation in the various task manager processes in the open() call
  @transient private var avroDeserializer: AvroDataToCatalyst = _
  @transient private var sparkRowDeser: ExpressionEncoder.Deserializer[Row] = _

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val query: Query = groupBy.streamingSource.get.getEvents.query

  private val timeColumnAlias: String = Constants.TimeColumn
  private val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)
  private val transforms: Seq[(String, String)] =
    (query.selects.toScala ++ Map(timeColumnAlias -> timeColumn)).toSeq
  private val filters: Seq[String] = query.getWheres.toScala

  @transient private var catalystUtil: CatalystUtil = _
  @transient private var rowSerializer: ExpressionEncoder.Serializer[Row] = _

  // various metrics
  @transient private var deserializationErrorCounter: Counter = _
  @transient private var exprEvalTimeHistogram: Histogram = _
  @transient private var rowSerTimeHistogram: Histogram = _
  @transient private var exprEvalSuccessCounter: Counter = _
  @transient private var exprEvalErrorCounter: Counter = _

  def encoder: Encoder[Row] = AvroCatalystUtils.buildEncoder(jsonSchema)

  // Chronon's CatalystUtil expects a Chronon `StructType` so we convert the
  // input event's schema to one.
  private val chrononInputEventSchema: ChrononStructType =
    ChrononStructType.from(
      s"${groupBy.metaData.cleanName}",
      SparkConversions.toChrononSchema(encoder.schema)
    )

  override def projectedSchema: Array[(String, DataType)] = {
    // before we do anything, run our setup statements.
    // in order to create the output schema, we'll evaluate expressions
    val sparkSchema = new CatalystUtil(chrononInputEventSchema, transforms, filters, groupBy.setups).getOutputSparkSchema
    sparkSchema.fields.map { field =>
      (field.name, SparkConversions.toChrononType(field.name, field.dataType))
    }
  }

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    val metricsGroup = context.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)

    deserializationErrorCounter = metricsGroup.counter("avro_deserialization_errors")
    avroDeserializer = AvroCatalystUtils.buildAvroDataToCatalyst(jsonSchema)

    val encoder: Encoder[Row] = AvroCatalystUtils.buildEncoder(jsonSchema)
    sparkRowDeser = encoder.asInstanceOf[ExpressionEncoder[Row]].resolveAndBind().createDeserializer()

    // spark expr eval vars
    catalystUtil = new CatalystUtil(chrononInputEventSchema, transforms, filters, groupBy.setups)
    val eventExprEncoder = encoder.asInstanceOf[ExpressionEncoder[Row]]
    rowSerializer = eventExprEncoder.createSerializer()

    exprEvalTimeHistogram = metricsGroup.histogram(
      "spark_expr_eval_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
    rowSerTimeHistogram = metricsGroup.histogram(
      "spark_row_ser_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
    exprEvalSuccessCounter = metricsGroup.counter("spark_expr_eval_success")
    exprEvalErrorCounter = metricsGroup.counter("spark_expr_eval_errors")
  }

  override def deserialize(messageBytes: Array[Byte], out: Collector[Map[String, Any]]): Unit = {
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
    .map(m => sparkRowDeser(m))
    .recover { case e: Exception =>
      logger.error("Failed to deserialize InternalRow to Row", e)
      deserializationErrorCounter.inc()
      null
    }
    .foreach(row => doSparkExprEval(row, out))
  }

  override def deserialize(messageBytes: Array[Byte]): Map[String, Any] = {
    throw new UnsupportedOperationException("Use the deserialize(message: Array[Byte], out: Collector[Map[String, Any]]) method instead.")
  }

  private def doSparkExprEval(inputEvent: Row, out: Collector[Map[String, Any]]): Unit = {
    try {
      val start = System.currentTimeMillis()
      val row: InternalRow = rowSerializer(inputEvent)
      val serFinish = System.currentTimeMillis()
      rowSerTimeHistogram.update(serFinish - start)

      val maybeRow = catalystUtil.performSql(row)

      exprEvalTimeHistogram.update(System.currentTimeMillis() - serFinish)
      maybeRow.foreach(out.collect)
      exprEvalSuccessCounter.inc()
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error("Error evaluating Spark expression", e)
        exprEvalErrorCounter.inc()
    }
  }
}

object ProjectedAvroDeserializationSupport {
  def build(groupBy: GroupBy,
            jsonSchema: String,
            schemaRegistryWireFormat: Boolean = false): ProjectedAvroDeserializationSchema = {
    new ProjectedAvroDeserializationSchema(groupBy, jsonSchema, schemaRegistryWireFormat)
  }
}
