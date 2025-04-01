package ai.chronon.orchestration.utils

import ai.chronon.api
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.{PartitionRange, PartitionSpec, TableDependency, Window}
import WindowUtils.convertUnits

object DependencyResolver {

  private def minus(partition: String, offset: Window)(implicit partitionSpec: PartitionSpec): String = {
    if (partition == null) return null
    if (offset == null) return null
    partitionSpec.minus(partition, offset)
  }

  def add(a: Window, b: Window): Window = {
    if (a == null) return b
    if (b == null) return a
    if (a.timeUnit == b.timeUnit) return new Window(a.length + b.length, a.timeUnit)

    // a's timeUnit takes precedence
    val length = convertUnits(b, a.timeUnit).length + a.length
    new Window(length, a.timeUnit)
  }

  private def max(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].max(partition, cutOff)
  }

  private def min(partition: String, cutOff: String): String = {
    if (partition == null) return cutOff
    if (cutOff == null) return partition
    Ordering[String].min(partition, cutOff)
  }

  def tableDependency(source: api.Source,
                      startOffset: Window,
                      endOffset: Window,
                      isMutation: Boolean = false): TableDependency = {

    val startCutOff = source.query.getStartPartition
    val endCutOff = source.query.getEndPartition

    val hasMutationTable = source.isSetEntities && source.getEntities.isSetMutationTable

    val table = if (isMutation) {
      assert(hasMutationTable, "No mutation table found for entity source, but mutation table dependency requested.")
      source.getEntities.getMutationTable
    } else {
      source.table
    }

    val result = new TableDependency()

    if (startOffset != null) result.setStartOffset(startOffset)
    if (endOffset != null) result.setEndOffset(endOffset)
    if (startCutOff != null) result.setStartCutOff(startCutOff)
    if (endCutOff != null) result.setEndCutOff(endCutOff)

    result.tableInfo.setIsCumulative(source.isCumulative)
    result.tableInfo.setTable(table)

    result
  }

  // return type for inputPartitionRange
  sealed trait PartitionRangeNeeded
  case class LatestPartitionInRange(start: String, end: String) extends PartitionRangeNeeded
  case class AllPartitionsInRange(start: String, end: String) extends PartitionRangeNeeded
  case object NoPartitions extends PartitionRangeNeeded

  def inputPartitionRange(queryRange: PartitionRange, tableDep: TableDependency)(implicit
      partitionSpec: PartitionSpec): PartitionRangeNeeded = {

    require(queryRange != null, "Query range cannot be null")
    require(queryRange.start != null, "Query range start cannot be null")
    require(queryRange.end != null, "Query range end cannot be null")

    val offsetStart = minus(queryRange.start, tableDep.getStartOffset)
    val offsetEnd = minus(queryRange.end, tableDep.getEndOffset)
    val start = max(offsetStart, tableDep.getStartCutOff)
    val end = min(offsetEnd, tableDep.getEndCutOff)

    if (start != null && end != null && start > end) {
      return NoPartitions
    }

    if (tableDep.tableInfo.isCumulative) {
      return LatestPartitionInRange(end, tableDep.getEndCutOff)
    }

    AllPartitionsInRange(start, end)
  }
}
