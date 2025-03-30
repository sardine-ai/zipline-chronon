package ai.chronon.aggregator.lambda
import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.api.{Aggregation, AggregationPart, DataType, Row, StructField}
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}

import java.util.{HashMap => JHashMap, Map => JMap}

case class Agg(aggPart: AggregationPart, columnAggregator: ColumnAggregator)

case class MiddleOutIr(middle: Any, tail: JMap[Long, Any])

class MiddleOutAggregator(inputSchema: Seq[(String, DataType)],
                          aggs: Seq[Aggregation],
                          middleTs: Long,
                          tailSizeMs: Long) {

  private val aggParts: Array[AggregationPart] = aggs.flatMap(_.unpack).toArray
  val rowAggregator = new RowAggregator(inputSchema, aggParts)

  val irSchema: Array[(String, DataType)] = rowAggregator.irSchema
  val outputSchema: Array[(String, DataType)] = rowAggregator.outputSchema
  val columnAggregators: Array[ColumnAggregator] = rowAggregator.columnAggregators
  val size: Int = columnAggregators.length

  case class WindowInfo(tailStart: Long, tailEnd: Long, tailHopMillis: Long)

  private case class AggInfo(columnAggregator: ColumnAggregator, aggPart: AggregationPart, windowInfo: WindowInfo)

  private val aggInfos: Array[AggInfo] = aggParts.zipWithIndex.map { case (p, idx) =>
    val windowInfo =
      if (p.window == null) null
      else {
        val windowMs = p.window.millis
        val tailStart = middleTs - windowMs
        val tailEnd = Math.min(tailSizeMs + tailStart, middleTs)
        WindowInfo(tailStart, tailEnd, windowMs)
      }
    AggInfo(columnAggregators(idx), aggParts(idx), windowInfo)
  }

  def init: Array[MiddleOutIr] = {
    columnAggregators.map(null)
  }

  def update(middleOutIr: Array[MiddleOutIr], row: Row): Unit = {
//      var i =
  }

}
