package ai.chronon.flink.window

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Constants
import ai.chronon.api.DataType
import ai.chronon.api.GroupBy
import ai.chronon.api.Row
import ai.chronon.api.ScalaJavaConversions.{IteratorOps, ListOps}
import ai.chronon.flink.types.TimestampedIR
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.TileCodec
import ai.chronon.online.serde.ArrayRow
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.{lang, util}
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.Seq

/** Wrapper Flink aggregator around Chronon's RowAggregator. Relies on Flink to pass in
  * the correct set of events for the tile. As the aggregates produced by this function
  * are used on the serving side along with other pre-aggregates, we don't 'finalize' the
  * Chronon RowAggregator and instead return the intermediate representation.
  *
  * (This cannot be a RichAggregateFunction because Flink does not support Rich functions in windows.)
  */
class FlinkRowAggregationFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)]
) extends AggregateFunction[Map[String, Any], TimestampedIR, TimestampedIR] {
  @transient private[flink] var rowAggregator: RowAggregator = _
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray // column order matters
  private val timeColumnAlias: String = Constants.TimeColumn

  private val isMutation: Boolean = {
    Option(groupBy.getSources).exists(
      _.iterator().toScala
        .exists(source => source.isSetEntities && source.getEntities.isSetMutationTopic)
    )
  }

  private val reversalIndex = {
    val result = inputSchema.indexWhere(_._1 == Constants.ReversalColumn)

    if (isMutation)
      require(result >= 0,
              s"Please specify source.query.reversal_column for CDC sources, only found, ${inputSchema.map(_._1)}")

    result
  }

  /*
   * Initialize the transient rowAggregator.
   * Running this method is an idempotent operation:
   *   1. The initialized RowAggregator is always the same given a `groupBy` and `inputSchema`.
   *   2. The RowAggregator itself doens't hold state; Flink keeps track of the state of the IRs.
   */
  private def initializeRowAggregator(): Unit =
    rowAggregator = TileCodec.buildRowAggregator(groupBy, inputSchema)

  override def createAccumulator(): TimestampedIR = {
    initializeRowAggregator()
    new TimestampedIR(rowAggregator.init, None)
  }

  override def add(
      element: Map[String, Any],
      accumulatorIr: TimestampedIR
  ): TimestampedIR = {

    // Most times, the time column is a Long, but it could be a Double.
    val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
      .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
    val row = toChrononRow(element, tsMills)

    // Given that the rowAggregator is transient, it may be null when a job is restored from a checkpoint
    if (rowAggregator == null) {
      logger.debug(
        f"The Flink RowAggregator was null for groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills"
      )
      initializeRowAggregator()
    }

    logger.debug(
      f"Flink pre-aggregates BEFORE adding new element: accumulatorIr=[${accumulatorIr.ir
        .mkString(", ")}] groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
    )

    val partialAggregates = Try {
      val isDelete = isMutation && row.getAs[Boolean](reversalIndex)

      if (isDelete) {
        rowAggregator.delete(accumulatorIr.ir, row)
      } else {
        rowAggregator.update(accumulatorIr.ir, row)
      }

    }

    partialAggregates match {
      case Success(v) => {
        logger.debug(
          f"Flink pre-aggregates AFTER adding new element [${v.mkString(", ")}] " +
            f"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
        )
        new TimestampedIR(v, Some(tsMills))
      }
      case Failure(e) =>
        logger.error(
          "Flink error calculating partial row aggregate. " +
            s"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element",
          e
        )
        throw e
    }
  }

  // Note we return intermediate results here as the results of this
  // aggregator are used on the serving side along with other pre-aggregates
  override def getResult(accumulatorIr: TimestampedIR): TimestampedIR =
    accumulatorIr

  override def merge(aIr: TimestampedIR, bIr: TimestampedIR): TimestampedIR =
    new TimestampedIR(
      rowAggregator.merge(aIr.ir, bIr.ir),
      aIr.latestTsMillis
        .flatMap(aL => bIr.latestTsMillis.map(bL => Math.max(aL, bL)))
        .orElse(aIr.latestTsMillis.orElse(bIr.latestTsMillis))
    )

  def toChrononRow(value: Map[String, Any], tsMills: Long): Row = {
    // The row values need to be in the same order as the input schema columns
    // The reason they are out of order in the first place is because the CatalystUtil does not return values in the
    // same order as the schema
    val values: Array[Any] = valueColumns.map(value(_))
    new ArrayRow(values, tsMills)
  }
}

// This process function is only meant to be used downstream of the ChrononFlinkAggregationFunction
class FlinkRowAggProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)]
) extends ProcessWindowFunction[TimestampedIR, TimestampedTile, java.util.List[Any], TimeWindow] {

  @transient private[flink] var tileCodec: TileCodec = _
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  @transient private var rowProcessingErrorCounter: Counter = _
  @transient private var eventProcessingErrorCounter: Counter =
    _ // Shared metric for errors across the entire Flink app.

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    tileCodec = new TileCodec(groupBy, inputSchema)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    rowProcessingErrorCounter = metricsGroup.counter("tiling_process_function_error")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
  }

  /** Process events emitted from the aggregate function.
    * Output format: (keys, encoded tile IR, timestamp of the event being processed)
    */
  override def process(
      keys: java.util.List[Any],
      context: ProcessWindowFunction[TimestampedIR, TimestampedTile, java.util.List[Any], TimeWindow]#Context,
      elements: lang.Iterable[TimestampedIR],
      out: Collector[TimestampedTile]): Unit = {
    val windowEnd = context.window.getEnd
    val irEntry = elements.iterator.next()
    val isComplete = context.currentWatermark >= windowEnd

    val tileBytes = Try {
      tileCodec.makeTileIr(irEntry.ir, isComplete)
    }

    tileBytes match {
      case Success(v) => {
        logger.debug(
          s"""
             |Flink aggregator processed element irEntry=$irEntry
             |tileBytes=${java.util.Base64.getEncoder.encodeToString(v)}
             |windowEnd=$windowEnd groupBy=${groupBy.getMetaData.getName}
             |keys=$keys isComplete=$isComplete tileAvroSchema=${tileCodec.tileAvroSchema}"""
        )
        // The timestamp should never be None here.
        out.collect(new TimestampedTile(keys, v, irEntry.latestTsMillis.get))
      }
      case Failure(e) =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error("Flink process error making tile IR", e)
        eventProcessingErrorCounter.inc()
        rowProcessingErrorCounter.inc()
    }
  }

}
