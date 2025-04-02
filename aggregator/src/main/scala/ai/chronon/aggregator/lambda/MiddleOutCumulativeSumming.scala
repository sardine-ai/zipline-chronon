package ai.chronon.aggregator.lambda
import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, Resolution}
import ai.chronon.api.{Aggregation, AggregationPart, DataType, Row, StructField, TimeUnit, TsUtils, Window}
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import scala.collection.mutable
import java.util.{HashMap => JHashMap, Map => JMap}

case class Agg(aggPart: AggregationPart, columnAggregator: ColumnAggregator)

class MiddleOutAggregator(inputSchema: Seq[(String, DataType)],
                          aggs: Seq[Aggregation],
                          uploadTs: Long,
                          tailSizeMs: Long = new Window(2, TimeUnit.DAYS).millis,
                          resolution: Resolution = FiveMinuteResolution) {

  private val aggParts: Array[AggregationPart] = aggs.flatMap(_.unpack).toArray
  val rowAggregator = new RowAggregator(inputSchema, aggParts)

  val irSchema: Array[(String, DataType)] = rowAggregator.irSchema
  val outputSchema: Array[(String, DataType)] = rowAggregator.outputSchema
  val columnAggregators: Array[ColumnAggregator] = rowAggregator.columnAggregators
  val size: Int = columnAggregators.length

  private case class WindowInfo(tailStart: Long, tailEnd: Long, tailHopMillis: Long)

  private case class AggInfo(columnAggregator: ColumnAggregator,
                             aggPart: AggregationPart,
                             windowInfo: WindowInfo,
                             idx: Int) {

    @inline
    def tailHopStart(ts: Long): Long = {
      TsUtils.round(ts, windowInfo.tailHopMillis)
    }

    @inline
    def inTail(ts: Long): Boolean = {
      val windowed = windowInfo != null
      lazy val inRange = windowInfo.tailStart <= ts && ts < windowInfo.tailEnd
      windowed && inRange
    }

    @inline
    def beforeTail(ts: Long): Boolean = {
      val windowed = windowInfo != null
      lazy val beforeTailStart = ts < windowInfo.tailStart
      windowed && beforeTailStart
    }

    @inline
    def inMiddle(ts: Long): Boolean = {
      val beforeMiddle = ts < uploadTs
      lazy val afterTail = windowInfo == null || windowInfo.tailEnd <= ts
      beforeMiddle && afterTail
    }
  }

  private val aggInfos: Array[AggInfo] = aggParts.zipWithIndex.map { case (p, idx) =>
    val windowInfo =
      if (p.window == null) null
      else {

        val windowMs = p.window.millis
        val tailStart = uploadTs - windowMs

        val tailEnd = Math.min(tailSizeMs + tailStart, uploadTs)
        val tailHopSize = resolution.calculateTailHop(p.window)

        WindowInfo(tailStart, tailEnd, tailHopSize)
      }

    AggInfo(columnAggregators(idx), aggParts(idx), windowInfo, idx)
  }

  case class MiddleOutIr(var middle: Any, var middleStartTs: Long, var tailHops: JMap[Long, Any]) {

    def updateMiddle(newMiddle: Any, eventTs: Long): Unit = {
      middle = newMiddle

      // We can use this timestamp to determine if a row has changed between batch uploads.
      // TODO: Use these timestamps for incremental uploads (replacing full batch uploads)
      middleStartTs = if (middleStartTs == -1L) eventTs else Math.min(middleStartTs, eventTs)
    }

  }

  case class SortedSuffixSumIr(startTimestamps: Array[Long], irs: Array[Any], endTimestamp: Long)

  object SortedSuffixSumIr {

    def from(tailHops: JMap[Long, String], )
    // cannot use middleOutIr after this.
    def from(aggInfo: AggInfo, middleOutIr: MiddleOutIr, maxEventTs: Long): SortedSuffixSumIr = {
      val window = aggInfo.aggPart.window
      if (window == null) {
        // no tail hops,
        SortedSuffixSumIr(Array(maxEventTs), Array(middleOutIr.middle), -1L)

      } else {

        val windowMillis = window.millis

        // going past latestValidQuery would result in stale data
        val latestValidQuery =
          TsUtils.roundUp(middleOutIr.middleStartTs + windowMillis, aggInfo.windowInfo.tailHopMillis)

        (middleOutIr.tailHops, middleOutIr.middle) match {
          case (null, null) => null
          case (null, middle) => SortedSuffixSumIr(Array(uploadTs), Array(middle), latestValidQuery)
          case (tail, null) => SortedSuffixSumIr.from
        }

        if (middleOutIr.tailHops == null) return SortedSuffixSumIr(Array(uploadTs), Array(middleOutIr.middle), latestValidQuery)

        if (middleOutIr.middle != null) {
          middleOutIr.tailHops
        }


        val hopEntries = middleOutIr.tailHops.entrySet()
        val hopIterator = hopEntries.iterator().toScala.filter(_.getValue != null)
        val sortedTailHopTimestamps: Array[Long] = hopIterator.map(_.getKey).toArray.sorted


        var runningSum = middleOutIr.middle
        sortedTailHopTimestamps.reverseIterator.map{ ts =>
          val ir = middleOutIr.tailHops.get(ts)
          val merged = if(ir == null) runningSum else aggInfo.columnAggregator.merge(ir, runningSum)
          runningSum = merged

        }
      }
    }
  }

  private object MiddleOutIr {
    def init: MiddleOutIr = MiddleOutIr(null, -1L, null)
  }

  def init: MiddleOutIrs = {
    MiddleOutIrs(Array.fill[MiddleOutIr](size)(null), null)
  }

  @inline
  private def updateCol(aggInfo: AggInfo, row: Row, existingIr: MiddleOutIr): MiddleOutIr = {
    val ts = row.ts

    if (ts > uploadTs || aggInfo.beforeTail(ts)) return existingIr

    val inputIdx = aggInfo.columnAggregator.columnIndices.input
    if (row.get(inputIdx) == null) return existingIr

    val result = if (existingIr != null) existingIr else MiddleOutIr.init

    if (aggInfo.inMiddle(ts)) {

      val newMiddle = aggInfo.columnAggregator.updateCol(result.middle, row)
      result.updateMiddle(newMiddle, row.ts)

    } else if (aggInfo.inTail(ts)) {

      if (result.tailHops == null) result.tailHops = new JHashMap[Long, Any]()

      val hopStart = aggInfo.tailHopStart(ts)
      val existingHop = result.tailHops.get(hopStart)
      val updatedHop = aggInfo.columnAggregator.updateCol(existingHop, row)

      result.tailHops.put(hopStart, updatedHop)
    }

    result
  }

  case class MiddleOutIrs(colIrs: Array[MiddleOutIr], var maxEventTs: Long)

  def update(middleOutIrs: MiddleOutIrs, row: Row): Unit = {
    if (row.ts > uploadTs) return

    var i = 0
    while (i < size) {
      val aggInfo = aggInfos(i)
      val newIr = updateCol(aggInfo, row, middleOutIrs.colIrs(i))
      middleOutIrs.colIrs.update(i, newIr)
      i += 1
    }

    middleOutIrs.maxEventTs = Math.max(row.ts, middleOutIrs.maxEventTs)
  }

  // already null checked by caller
  private def mergeTailMaps(aggInfo: AggInfo, dst: JMap[Long, Any], src: JMap[Long, Any]): JMap[Long, Any] = {
    val it = src.entrySet().iterator()
    while (it.hasNext) {
      val tsAndIr = it.next()

      val ts = tsAndIr.getKey
      val srcIr = tsAndIr.getValue

      val existingHop = dst.get(ts)
      val newHop = aggInfo.columnAggregator.merge(existingHop, srcIr)

      dst.put(ts, newHop)
    }

    dst
  }

  @inline
  private def mergeIntoCol(aggInfo: AggInfo, dst: MiddleOutIr, src: MiddleOutIr): MiddleOutIr = {
    if (dst == null) return src
    if (src == null) return dst

    dst.middle = aggInfo.columnAggregator.merge(dst.middle, src.middle)

    dst.tailHops = if (dst.tailHops == null) {
      src.tailHops
    } else if (src.tailHops == null) {
      dst.tailHops
    } else {
      mergeTailMaps(aggInfo, dst.tailHops, src.tailHops)
    }

    dst
  }

  // will modify the dst in place
  def mergeInto(dst: Array[MiddleOutIr], src: Array[MiddleOutIr]): Array[MiddleOutIr] = {
    if (dst == null) return src
    if (src == null) return dst

    var i = 0
    while (i < size) {
      val aggInfo = aggInfos(i)
      dst.update(i, mergeIntoCol(aggInfo, dst(i), src(i)))
      i += 1
    }

    dst
  }

  // array of timestamp & ir
  private def flattenCol(aggInfo: AggInfo, ir: MiddleOutIr): Array[(Long, Any)] = {
    val base = ir.middle

  }

  case class IrWithRange(ir: Any, queryStartMs: Long, queryEndMs: Long)
  @inline
  private def flatten(ir: Array[MiddleOutIr]): Array[(Long, Array[Any])] = {}

}
