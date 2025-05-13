package ai.chronon.flink.deser

import ai.chronon.api.{DataType, GroupBy}
import ai.chronon.flink.SparkExpressionEval
import ai.chronon.online.serde.{Mutation, SerDe, SparkConversions}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.slf4j.{Logger, LoggerFactory}

abstract class BaseDeserializationSchema[T](deserSchemaProvider: SerDe, groupBy: GroupBy)
    extends ChrononDeserializationSchema[T] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // these are created on instantiation in the various task manager processes in the open() call
  @transient protected var deserializationErrorCounter: Counter = _

  override def sourceProjectionEnabled: Boolean = false

  override def sourceEventEncoder: Encoder[Row] =
    Encoders.row(SparkConversions.fromChrononSchema(deserSchemaProvider.schema))

  override def open(context: DeserializationSchema.InitializationContext): Unit = {
    super.open(context)
    val metricsGroup = context.getMetricGroup
      .addGroup("chronon")
      .addGroup("group_by", groupBy.getMetaData.getName)
    deserializationErrorCounter = metricsGroup.counter("deserialization_errors")
  }

  protected def doDeserializeMutation(messageBytes: Array[Byte]): Option[Mutation] = {
    try {
      Some(deserSchemaProvider.fromBytes(messageBytes))
    } catch {
      case e: Exception =>
        logger.error("Error deserializing message", e)
        deserializationErrorCounter.inc()
        None
    }
  }
}

class SourceIdentityDeserializationSchema(deserSchemaProvider: SerDe, groupBy: GroupBy)
    extends BaseDeserializationSchema[Row](deserSchemaProvider, groupBy) {

  override def deserialize(messageBytes: Array[Byte], out: Collector[Row]): Unit = {
    val maybeMutation = doDeserializeMutation(messageBytes)

    maybeMutation.foreach { mutation =>
      Seq(mutation.before, mutation.after)
        .filter(_ != null)
        .map(r => SparkConversions.toSparkRow(r, deserSchemaProvider.schema, GenericRowHandler.func).asInstanceOf[Row])
        .foreach(row => out.collect(row))
    }
  }

  override def deserialize(message: Array[Byte]): Row = {
    throw new UnsupportedOperationException(
      "Use the deserialize(message: Array[Byte], out: Collector[Row]) method instead.")
  }
}

class SourceProjectionDeserializationSchema(deserSchemaProvider: SerDe, groupBy: GroupBy)
    extends BaseDeserializationSchema[Map[String, Any]](deserSchemaProvider, groupBy)
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

    performSqlErrorCounter = metricsGroup.counter("sql_exec_errors")

    // spark expr eval vars
    val eventExprEncoder = sourceEventEncoder.asInstanceOf[ExpressionEncoder[Row]]
    rowSerializer = eventExprEncoder.createSerializer()
    evaluator = new SparkExpressionEval[Row](sourceEventEncoder, groupBy)
    evaluator.initialize(metricsGroup)
  }

  override def deserialize(messageBytes: Array[Byte], out: Collector[Map[String, Any]]): Unit = {
    val maybeMutation = doDeserializeMutation(messageBytes)

    val mutations = maybeMutation
      .map { mutation =>
        Seq(mutation.before, mutation.after).filter(_ != null)
      }
      .getOrElse(Seq.empty)

    mutations.foreach(row => doSparkExprEval(row, out))
  }

  override def deserialize(messageBytes: Array[Byte]): Map[String, Any] = {
    throw new UnsupportedOperationException(
      "Use the deserialize(message: Array[Byte], out: Collector[Map[String, Any]]) method instead.")
  }

  private def doSparkExprEval(inputEvent: Array[Any], out: Collector[Map[String, Any]]): Unit = {
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

object GenericRowHandler {
  val func: Any => Array[Any] = {
    case x: GenericRowWithSchema => {
      val result = new Array[Any](x.length)
      var i = 0
      while (i < x.length) {
        result.update(i, x.get(i))
        i += 1
      }
      result
    }
  }
}
