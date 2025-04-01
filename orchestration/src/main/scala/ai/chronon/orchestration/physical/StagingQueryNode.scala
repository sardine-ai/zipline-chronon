package ai.chronon.orchestration.physical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{StagingQuery, TableDependency}
import ai.chronon.orchestration.PhysicalNodeType
import ai.chronon.orchestration.StagingQueryNodeType
import ai.chronon.orchestration.utils
import ai.chronon.api.CollectionExtensions.JListExtension
import ai.chronon.orchestration.utils.ShiftConstants.noShift

class StagingQueryNode(stagingQuery: StagingQuery) extends TabularNode[StagingQuery](stagingQuery) {
  override def nodeType: PhysicalNodeType = utils.PhysicalNodeType.from(StagingQueryNodeType.BACKFILL)

  override def tableDependencies: Seq[TableDependency] = {
    val deps = Seq.empty[String] // TODO: stagingQuery.metaData.dependencies

    // example format of dependencies: "sample_namespace.sample_table/ds={{ ds }}"
    // TODO: fix the assumption below later - there is ParametricMacro class that contains some information
    // we will assume that startOffset is null, and endOffset is zero
    deps.map { dep =>
      {
        val tableName = dep.split("/")(0)
        val result = new TableDependency()
        result.setStartOffset(noShift)
        result.setEndOffset(noShift)
        result.tableInfo.setIsCumulative(false)
        result.tableInfo.setTable(tableName)
        result
      }
    }.toSeq

  }

  override def outputTable: String = stagingQuery.metaData.outputTable
}
