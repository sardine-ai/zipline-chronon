package ai.chronon.orchestration.utils

import ai.chronon.api.Extensions.JoinSourceOps
import ai.chronon.api.Source
import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.LogicalType
import ai.chronon.orchestration.TabularData
import ai.chronon.orchestration.TabularDataType

object Config {
  def getType(config: LogicalNode): LogicalType = {
    config match {
      case c if c.isSetJoin         => LogicalType.JOIN
      case c if c.isSetGroupBy      => LogicalType.GROUP_BY
      case c if c.isSetStagingQuery => LogicalType.STAGING_QUERY
      case c if c.isSetModel        => LogicalType.MODEL
    }
  }

  def from(table: String, tableType: TabularDataType): LogicalNode = {
    val td = new TabularData()
    td.setTable(table)
    td.setType(tableType)

    val config = new LogicalNode()
    config.setTabularData(td)
    config
  }

  def from(source: Source): LogicalNode = {
    val td = new TabularData()

    val effectiveSource = if (source.isSetJoinSource) {
      source.getJoinSource.toDirectSource
    } else {
      source
    }

    if (effectiveSource.isSetEvents) {
      val events = effectiveSource.getEvents

      td.setTable(events.getTable)
      td.setTopic(events.getTopic)

      if (events.isCumulative) {
        td.setType(TabularDataType.CUMULATIVE_EVENTS)
      } else {
        td.setType(TabularDataType.EVENT)
      }

    } else {
      val entities = effectiveSource.getEntities

      td.setTable(entities.getSnapshotTable)
      td.setTopic(entities.getMutationTopic)
      td.setMutationTable(entities.getMutationTable)
      td.setType(TabularDataType.ENTITY)
    }

    val config = new LogicalNode()
    config.setTabularData(td)
    config
  }
}
