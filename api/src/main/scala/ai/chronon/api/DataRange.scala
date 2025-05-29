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

//TODO: Use Options all the way through
case class TimeRange(start: Long, end: Long)(implicit partitionSpec: PartitionSpec) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(TsUtils.round(start, partitionSpec.spanMillis))(_ + partitionSpec.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(Option(partitionSpec.at(start)), Option(partitionSpec.at(end)), partitionSpec)
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
  override def toString: String = s"[${TsUtils.toStr(start)}-${TsUtils.toStr(end)}]"
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: Option[String], end: Option[String], partitionSpec: PartitionSpec)
    extends DataRange
    with Ordered[PartitionRange] {

  def isSingleDay: Boolean = {
    start == end
  }

  def intersect(other: PartitionRange): PartitionRange = {
    // lots of null handling
    val newStart = (start ++ other.start)
      .reduceLeftOption(Ordering[String].max)
    val newEnd = (end ++ other.end)
      .reduceLeftOption(Ordering[String].min)
    // could be invalid
    PartitionRange(newStart, newEnd, partitionSpec)
  }

  override def toTimePoints: Array[Long] = {
    require(start.isDefined && end.isDefined, "Can't request timePoint conversion when PartitionRange is unbounded")
    Stream
      .iterate(start.get)(partitionSpec.after)
      .takeWhile(_ <= end.get)
      .map(partitionSpec.epochMillis)
      .toArray
  }

  def betweenClauses: String = {
    require(wellDefined, "Can't request betweenClauses when PartitionRange is unbounded")
    s"${partitionSpec.column} BETWEEN '${start.get}' AND '${end.get}'"
  }

  def whereClauses: Seq[String] = {
    (start.map(s => s"${partitionSpec.column} >= '$s'") ++ end.map(e => s"${partitionSpec.column} <= '$e'")).toSeq
  }

  def steps(days: Int): Seq[PartitionRange] = {
    partitions
      .sliding(days, days) // sliding(x, x) => tumbling(x)
      .map { step => PartitionRange(Option(step.head), Option(step.last), partitionSpec) }
      .toSeq
  }

  def partitions: Seq[String] = {
    require(wellDefined, s"Invalid partition range $this")
    Stream
      .iterate(start.get)(partitionSpec.after)
      .takeWhile(_ <= end.get)
  }

  // no nulls in start or end and start <= end - used as a pre-check before the `partitions` function
  def wellDefined: Boolean = start.isDefined && end.isDefined && start.get <= end.get

  def shift(days: Int): PartitionRange = {
    if (days == 0) {
      this
    } else {
      PartitionRange(start.map(partitionSpec.shift(_, days)), end.map(partitionSpec.shift(_, days)), partitionSpec)
    }
  }

  def shiftMillis(millis: Long): PartitionRange = {
    val newStart = start.map(partitionSpec.shift(_, millis))
    val newEnd = end.map(partitionSpec.shift(_, millis))
    PartitionRange(newStart, newEnd, partitionSpec)
  }

  override def compare(that: PartitionRange): Int = {
    def compareDate(left: Option[String], right: Option[String]): Int = {
      (left, right) match {
        case (Some(_), None)              => 1
        case (None, Some(_))              => -1
        case (Some(l), Some(r)) if l == r => 0
        case (Some(l), Some(r))           => l compareTo right
        case (None, None)                 => 0
      }
    }
    val compareStart = compareDate(this.start, that.start)
    if (compareStart != 0) {
      compareStart
    } else {
      compareDate(this.end, that.end)
    }
  }

  def translate(otherSpec: PartitionSpec): PartitionRange = {

    val newStart = start.map(d => partitionSpec.translate(d, otherSpec))
    val newEnd = end.map(d => partitionSpec.translate(d, otherSpec))

    PartitionRange(newStart, newEnd, otherSpec)
  }

  override def toString: String = s"[$start...$end]"
}

object PartitionRange {
  def rangesToString(ranges: Iterable[PartitionRange]): String = {
    val tuples = ranges.map(r => s"(${r.start} -> ${r.end})").mkString(", ")
    s"$tuples"
  }

  def fromString(partitions: Seq[String], partitionSpec: PartitionSpec): Seq[PartitionRange] = {
    val sortedDates = partitions.sorted
    sortedDates.foldLeft(Seq[PartitionRange]()) { (ranges, nextDate) =>
      if (ranges.isEmpty || ranges.last.end.map(partitionSpec.after(_) != nextDate).get) {
        ranges :+ PartitionRange(Option(nextDate), Option(nextDate), partitionSpec)
      } else {
        val newRange = PartitionRange(ranges.last.start, Option(nextDate), partitionSpec)
        ranges.dropRight(1) :+ newRange
      }
    }
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
        val range = PartitionRange(Option(start), Option(end), partitionSpec)
        result = result :+ range
        start = p
        end = p
      }
    }
    result :+ PartitionRange(Option(start), Option(end), partitionSpec)
  }

  def collapsedPrint(parts: Iterable[String])(implicit partitionSpec: PartitionSpec): String = {
    val ranges = collapseToRange(parts)
    if (ranges == null) return ""
    rangesToString(ranges)
  }

  def toTimeRange(partitionRange: PartitionRange): TimeRange = {
    val spec = partitionRange.partitionSpec
    // TODO: use Options all the way through
    val shiftedEnd = spec.after(partitionRange.end.orNull)
    TimeRange(spec.epochMillis(partitionRange.start.orNull), spec.epochMillis(shiftedEnd) - 1)(spec)
  }
}
