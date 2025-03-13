package ai.chronon.orchestration.physical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{GroupBy, TableDependency}

class GroupByPrepareUpload(groupBy: GroupBy) extends GroupByBackfill(groupBy) {
  override def outputTable: String = groupBy.metaData.outputTable

  // same table deps as backfill
  override def tableDependencies: Seq[TableDependency] = super.tableDependencies
}
