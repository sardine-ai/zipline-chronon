package ai.chronon.orchestration.logical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.StagingQuery
import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularDataType
import ai.chronon.orchestration.utils
import ai.chronon.orchestration.utils.CollectionExtensions.JListExtension

// StagingQuery implementation
case class StagingQueryNodeImpl(stagingQuery: StagingQuery) extends LogicalNodeImpl {
  override def name: String = stagingQuery.metaData.name

  override def outputTables: Seq[String] =
    Seq(stagingQuery.metaData.outputTable)

  override def toConfig: LogicalNode = {
    val config = new LogicalNode()
    config.setStagingQuery(stagingQuery)
    config
  }

  override def parents: Seq[LogicalNode] =
    stagingQuery.metaData.dependencies
      .map(dep => utils.Config.from(dep.split("/").head, TabularDataType.ENTITY))
      .toSeq

  override def semanticHash: String = {
    stagingQuery.deepCopy().unsetMetaData().hashCode().toHexString
  }

  // TODO: we need to get this from users, dummy-ed for now
  override def tabularDataType: TabularDataType = TabularDataType.ENTITY
}
