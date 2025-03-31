package ai.chronon.flink

import ai.chronon.api.GroupBy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.Seq

/** A Flink function that uses Chronon's CatalystUtil (via the SparkExpressionEval) to evaluate the Spark SQL expression in a GroupBy.
  * This function is instantiated for a given type T (specific case class object, Thrift / Proto object).
  * Based on the selects and where clauses in the GroupBy, this function projects and filters the input data and
  * emits a Map which contains the relevant fields & values that are needed to compute the aggregated values for the
  * GroupBy.
  * @param encoder Spark Encoder for the input data type
  * @param groupBy The GroupBy to evaluate.
  * @tparam T The type of the input data.
  */
class SparkExpressionEvalFn[T](encoder: Encoder[T], groupBy: GroupBy) extends RichFlatMapFunction[T, Map[String, Any]] {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var evaluator: SparkExpressionEval[T] = _
  @transient private var rowSerializer: ExpressionEncoder.Serializer[T] = _

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    val eventExprEncoder = encoder.asInstanceOf[ExpressionEncoder[T]]
    rowSerializer = eventExprEncoder.createSerializer()

    evaluator = new SparkExpressionEval[T](encoder, groupBy)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)

    evaluator.initialize(metricsGroup)
  }

  def flatMap(inputEvent: T, out: Collector[Map[String, Any]]): Unit = {
    evaluator.evaluateExpressions(inputEvent, rowSerializer).foreach(out.collect)
  }

  override def close(): Unit = {
    super.close()
    evaluator.close()
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. To do the Spark Df based eval, we:
  // 1. Create a df with the events + record_id tacked on
  // 2. Apply the projections and filters based on how we've set up the CatalystUtil instance based on the input groupBy.
  // 3. Collect the results and group them by record_id
  def runSparkSQLBulk(idToRecords: Seq[(String, Row)]): Map[String, Seq[Map[String, Any]]] = {
    evaluator.runSparkSQLBulk(idToRecords)
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. This method iterates over the input records and hits the catalyst performSql method
  // to collect results.
  def runCatalystBulk(records: Seq[(String, T)]): Map[String, Seq[Map[String, Any]]] = {
    evaluator.runCatalystBulk(records, rowSerializer)
  }
}
