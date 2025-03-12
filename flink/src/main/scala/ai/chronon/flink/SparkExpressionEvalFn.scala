package ai.chronon.flink

import ai.chronon.api.Constants
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.GroupBy
import ai.chronon.api.Query
import ai.chronon.api.{StructType => ChrononStructType}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.CatalystUtil
import ai.chronon.online.SparkConversions
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Counter
import org.apache.flink.metrics.Histogram
import org.apache.flink.util.Collector
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.Seq

/** A Flink function that uses Chronon's CatalystUtil to evaluate the Spark SQL expression in a GroupBy.
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

  private val query: Query = groupBy.streamingSource.get.getEvents.query

  private val timeColumnAlias: String = Constants.TimeColumn
  private val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)
  private val transforms: Seq[(String, String)] =
    (query.selects.toScala ++ Map(timeColumnAlias -> timeColumn)).toSeq
  private val filters: Seq[String] = query.getWheres.toScala

  @transient private var catalystUtil: CatalystUtil = _
  @transient private var rowSerializer: ExpressionEncoder.Serializer[T] = _

  @transient private var exprEvalTimeHistogram: Histogram = _
  @transient private var rowSerTimeHistogram: Histogram = _
  @transient private var exprEvalSuccessCounter: Counter = _
  @transient private var exprEvalErrorCounter: Counter = _

  // Chronon's CatalystUtil expects a Chronon `StructType` so we convert the
  // Encoder[T]'s schema to one.
  private val chrononSchema: ChrononStructType =
    ChrononStructType.from(
      s"${groupBy.metaData.cleanName}",
      SparkConversions.toChrononSchema(encoder.schema)
    )

  private[flink] def getOutputSchema: StructType = {
    // before we do anything, run our setup statements.
    // in order to create the output schema, we'll evaluate expressions
    new CatalystUtil(chrononSchema, transforms, filters, groupBy.setups).getOutputSparkSchema
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    catalystUtil = new CatalystUtil(chrononSchema, transforms, filters, groupBy.setups)
    val eventExprEncoder = encoder.asInstanceOf[ExpressionEncoder[T]]
    rowSerializer = eventExprEncoder.createSerializer()

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)

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

  def flatMap(inputEvent: T, out: Collector[Map[String, Any]]): Unit = {
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

  override def close(): Unit = {
    super.close()
    CatalystUtil.session.close()
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. To do the Spark Df based eval, we:
  // 1. Create a df with the events + record_id tacked on
  // 2. Apply the projections and filters based on how we've set up the CatalystUtil instance based on the input groupBy.
  // 3. Collect the results and group them by record_id
  def runSparkSQLBulk(idToRecords: Seq[(String, Row)]): Map[String, Seq[Map[String, Any]]] = {

    val idField = StructField("__record_id", StringType, false)
    val fullSchema = StructType(idField +: encoder.schema.fields)
    val fullRows = idToRecords.map { case (id, row) =>
      // Create a new Row with id as the first field, followed by all fields from the original row
      Row.fromSeq(id +: row.toSeq)
    }

    val rowsRdd = CatalystUtil.session.sparkContext.parallelize(fullRows.toSeq)

    val eventDfs = CatalystUtil.session
      .createDataFrame(rowsRdd, fullSchema)

    // Apply filtering if needed
    val filteredDf = catalystUtil.whereClauseOpt match {
      case Some(whereClause) => eventDfs.where(whereClause)
      case None              => eventDfs
    }

    // Apply projections while preserving the index
    val projectedDf = filteredDf.selectExpr(
      // Include the index column and all the select clauses
      Array("__record_id") ++ catalystUtil.selectClauses: _*
    )

    // Collect the results
    val results = projectedDf.collect()

    // Group results by record ID
    val resultsByRecordId = results.groupBy(row => row.getString(0))

    // Map back to the original record order
    idToRecords.map { record =>
      val recordId = record._1
      val resultRows = resultsByRecordId.getOrElse(recordId, Array.empty)

      val maps = resultRows.map { row =>
        val columnNames = projectedDf.columns.tail // Skip the record ID column
        columnNames.zipWithIndex.map { case (colName, i) =>
          (colName, row.get(i + 1)) // +1 to skip the record ID column
        }.toMap
      }.toSeq

      (recordId, maps)
    }.toMap
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. This method iterates over the input records and hits the catalyst performSql method
  // to collect results.
  def runCatalystBulk(records: Seq[(String, T)]): Map[String, Seq[Map[String, Any]]] = {
    records.map { record =>
      val recordId = record._1
      val row = rowSerializer(record._2)
      val maybeRow = catalystUtil.performSql(row)
      (recordId, maybeRow)
    }.toMap
  }
}
