/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

sealed trait DataRange {
  def toTimePoints: Array[Long]
}
case class TimeRange(start: Long, end: Long)(implicit partitionSpec: PartitionSpec) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(TsUtils.round(start, partitionSpec.spanMillis))(_ + partitionSpec.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(partitionSpec.at(start), partitionSpec.at(end))
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
  override def toString: String = s"[${TsUtils.toStr(start)}-${TsUtils.toStr(end)}]"
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: String, end: String)(implicit val partitionSpec: PartitionSpec)
    extends DataRange
    with Ordered[PartitionRange] {

  def valid: Boolean = {
    (Option(start), Option(end)) match {
      case (Some(s), Some(e)) => s <= e
      case _                  => true
    }
  }

  def isSingleDay: Boolean = {
    start == end
  }

  def intersect(other: PartitionRange): PartitionRange = {
    // lots of null handling
    val newStart = (Option(start) ++ Option(other.start))
      .reduceLeftOption(Ordering[String].max)
      .orNull
    val newEnd = (Option(end) ++ Option(other.end))
      .reduceLeftOption(Ordering[String].min)
      .orNull
    // could be invalid
    PartitionRange(newStart, newEnd)
  }

  override def toTimePoints: Array[Long] = {
    assert(start != null && end != null, "Can't request timePoint conversion when PartitionRange is unbounded")
    Stream
      .iterate(start)(partitionSpec.after)
      .takeWhile(_ <= end)
      .map(partitionSpec.epochMillis)
      .toArray
  }

  def betweenClauses(partitionColumn: String): String = {
    s"$partitionColumn BETWEEN '$start' AND '$end'"
  }

  def whereClauses(partitionColumn: String): Seq[String] = {
    (Option(start).map(s => s"$partitionColumn >= '$s'") ++ Option(end).map(e => s"$partitionColumn <= '$e'")).toSeq
  }

  def steps(days: Int): Seq[PartitionRange] = {
    partitions
      .sliding(days, days) // sliding(x, x) => tumbling(x)
      .map { step => PartitionRange(step.head, step.last) }
      .toSeq
  }

  def partitions: Seq[String] = {
    assert(wellDefined, s"Invalid partition range $this")
    Stream
      .iterate(start)(partitionSpec.after)
      .takeWhile(_ <= end)
  }

  // no nulls in start or end and start <= end - used as a pre-check before the `partitions` function
  def wellDefined: Boolean = start != null && end != null && start <= end

  def shift(days: Int): PartitionRange = {
    if (days == 0) {
      this
    } else {
      PartitionRange(partitionSpec.shift(start, days), partitionSpec.shift(end, days))
    }
  }

  override def compare(that: PartitionRange): Int = {
    def compareDate(left: String, right: String): Int = {
      if (left == right) {
        0
      } else if (left == null) {
        -1
      } else if (right == null) {
        1
      } else {
        left compareTo right
      }
    }

    val compareStart = compareDate(this.start, that.start)
    if (compareStart != 0) {
      compareStart
    } else {
      compareDate(this.end, that.end)
    }
  }
  override def toString: String = s"[$start...$end]"
}

object PartitionRange {
  def rangesToString(ranges: Iterable[PartitionRange]): String = {
    val tuples = ranges.map(r => s"(${r.start} -> ${r.end})").mkString(", ")
    s"$tuples"
  }

  // takes a list of partitions and collapses them into ranges
  // eg: ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-08"]
  // will return: [
  //    PartitionRange("2020-01-01", "2020-01-03"),
  //    PartitionRange("2020-01-05", "2020-01-05"),
  //    PartitionRange("2020-01-07", "2020-01-08")
  // ]
  def collapseToRange(partitions: Iterable[String])(implicit partitionSpec: PartitionSpec): Seq[PartitionRange] = {
    if (partitions == null) return null
    var result = Seq.empty[PartitionRange]
    val sortedPartitions = partitions.toSeq.sorted.distinct
    if (sortedPartitions.isEmpty) return result
    var start = sortedPartitions.head
    var end = start
    sortedPartitions.tail.foreach { p =>
      if (partitionSpec.after(end) == p) {
        end = p
      } else {
        val range = PartitionRange(start, end)
        result = result :+ range
        start = p
        end = p
      }
    }
    result :+ PartitionRange(start, end)
  }

  def collapsedPrint(parts: Iterable[String])(implicit partitionSpec: PartitionSpec): String = {
    val ranges = collapseToRange(parts)
    if (ranges == null) return ""
    rangesToString(ranges)
  }

  def toTimeRange(partitionRange: PartitionRange): TimeRange = {
    val spec = partitionRange.partitionSpec
    val shiftedEnd = spec.after(partitionRange.end)
    TimeRange(spec.epochMillis(partitionRange.start), spec.epochMillis(shiftedEnd) - 1)(spec)
  }
}
