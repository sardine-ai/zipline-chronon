package ai.chronon.api.planner

import ai.chronon.api
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.Extensions.WindowUtils.convertUnits
import ai.chronon.api.{PartitionRange, PartitionSpec, TableDependency, TableInfo, Window}

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

    val tableInfo = new TableInfo()
      .setIsCumulative(source.isCumulative)
      .setTable(table)

    result.setTableInfo(tableInfo)

    result
  }

  def computeInputRange(queryRange: PartitionRange, tableDep: TableDependency)(implicit
      partitionSpec: PartitionSpec): Option[PartitionRange] = {

    require(queryRange != null, "Query range cannot be null")
    require(queryRange.start != null, "Query range start cannot be null")
    require(queryRange.end != null, "Query range end cannot be null")

    val offsetStart = minus(queryRange.start, tableDep.getStartOffset)
    val offsetEnd = minus(queryRange.end, tableDep.getEndOffset)
    val start = max(offsetStart, tableDep.getStartCutOff)
    val end = min(offsetEnd, tableDep.getEndCutOff)

    if (start != null && end != null && start > end) {
      return None
    }

    if (tableDep.tableInfo.isCumulative) {

      // we should always compute the latest possible partition when end_cutoff is not set
      val latestValidInput = Option(tableDep.getEndCutOff).getOrElse(partitionSpec.now)
      val latestValidInputWithOffset = minus(latestValidInput, tableDep.getEndOffset)

      return Some(PartitionRange(latestValidInputWithOffset, latestValidInputWithOffset))

    }

    Some(PartitionRange(start, end))
  }

  def getMissingSteps(requiredPartitionRange: PartitionRange,
                      existingPartitions: Seq[String],
                      stepDays: Int = 1): Seq[PartitionRange] = {
    val requiredPartitions = requiredPartitionRange.partitions

    val missingPartitions = requiredPartitions.filterNot(existingPartitions.contains)
    val missingPartitionRanges = PartitionRange.collapseToRange(missingPartitions)(requiredPartitionRange.partitionSpec)

    val missingSteps = missingPartitionRanges.flatMap(_.steps(stepDays))
    missingSteps
  }
}
