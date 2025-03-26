package ai.chronon.orchestration.physical

import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.api.{GroupBy, TableDependency}
import ai.chronon.orchestration.GroupByNodeType
import ai.chronon.orchestration.PhysicalNodeType
import ai.chronon.orchestration.utils
import ai.chronon.api.CollectionExtensions.JListExtension
import ai.chronon.orchestration.utils.DependencyResolver.tableDependency
import ai.chronon.orchestration.utils.ShiftConstants.PartitionTimeUnit
import ai.chronon.orchestration.utils.ShiftConstants.noShift
import ai.chronon.orchestration.utils.WindowUtils

class GroupByBackfill(groupBy: GroupBy) extends TabularNode[GroupBy](groupBy) {
  override def outputTable: String = groupBy.metaData.uploadTable

  override def nodeType: PhysicalNodeType = utils.PhysicalNodeType.from(GroupByNodeType.SNAPSHOT)

  override def tableDependencies: Seq[TableDependency] = {

    groupBy.sources
      .map(rawSource => {

        val lookBack = groupBy.maxWindow.map(WindowUtils.convertUnits(_, PartitionTimeUnit))

        val source = if (rawSource.isSetJoinSource) rawSource.getJoinSource.toDirectSource else rawSource

        (source.dataModel, source.isCumulative) match {
          case (Events, false) => tableDependency(source, noShift, lookBack.orNull)
          case _               => tableDependency(source, noShift, noShift)
        }

      })
      .toSeq
  }
}
