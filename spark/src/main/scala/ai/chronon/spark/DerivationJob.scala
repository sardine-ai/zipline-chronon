package ai.chronon.spark

import ai.chronon.api
import ai.chronon.online.PartitionRange

import scala.jdk.CollectionConverters.asScalaBufferConverter

/*
For entities with Derivations (`GroupBy` and `Join`), we produce the pre-derivation `base` table first,
then the derivation job runs as an incremental step.
 */
class DerivationJob(sourceTable: String,
                    outputTable: String,
                    derivations: Seq[api.Derivation],
                    dateRange: PartitionRange) {
  def run(): Unit = {}
}

object DerivationJob {
  def fromGroupBy(groupBy: api.GroupBy, dateRange: PartitionRange): DerivationJob = {
    val baseOutputTable = "TODO" // Output of the base GroupBy pre-derivation
    val finalOutputTable = "TODO" // The actual output table
    val derivations = groupBy.derivations.asScala
    new DerivationJob(baseOutputTable, finalOutputTable, derivations, dateRange)
  }

  def fromJoin(join: api.Join, dateRange: PartitionRange): DerivationJob = {
    val baseOutputTable = "TODO" // Output of the base Join pre-derivation
    val finalOutputTable = "TODO" // The actual output table
    val derivations = join.derivations.asScala
    new DerivationJob(baseOutputTable, finalOutputTable, derivations, dateRange)
  }
}
