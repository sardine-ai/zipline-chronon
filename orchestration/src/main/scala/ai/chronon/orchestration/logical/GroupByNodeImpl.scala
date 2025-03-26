package ai.chronon.orchestration.logical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.GroupBy
import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularDataType
import ai.chronon.orchestration.utils
import ai.chronon.api.CollectionExtensions.JListExtension

// GroupBy implementation
case class GroupByNodeImpl(groupBy: GroupBy) extends LogicalNodeImpl {
  override def name: String = "group_bys." + groupBy.metaData.name

  override def outputTables: Seq[String] =
    Seq(groupBy.metaData.outputTable)

  override def toConfig: LogicalNode = {
    val config = new LogicalNode()
    config.setGroupBy(groupBy)
    config
  }

  override def parents: Seq[LogicalNode] = {
    groupBy.sources.map { utils.Config.from }.toSeq
  }

  override def semanticHash: String = {
    // function of everything except for metadata
    val baseGroupBy = groupBy.deepCopy()
    baseGroupBy.unsetMetaData()

    // remove joins within - it will be handled via lineage hash
    baseGroupBy.sources.foreach { s =>
      if (s.isSetJoinSource) s.getJoinSource.unsetJoin()
    }

    baseGroupBy.hashCode().toHexString
  }

  // groupBy output is always an entity
  override def tabularDataType: TabularDataType = TabularDataType.ENTITY
}
