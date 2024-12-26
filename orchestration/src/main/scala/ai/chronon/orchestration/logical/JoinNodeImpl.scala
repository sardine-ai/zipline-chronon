package ai.chronon.orchestration.logical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Join
import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularDataType
import ai.chronon.orchestration.utils
import ai.chronon.orchestration.utils.CollectionExtensions._
import ai.chronon.orchestration.utils.TabularDataUtils

// Join implementation
case class JoinNodeImpl(join: Join) extends LogicalNodeImpl {
  override def name: String = join.metaData.name

  override def outputTables: Seq[String] =
    Seq(join.metaData.outputTable, join.metaData.loggedTable)

  override def toConfig: LogicalNode = {
    val config = new LogicalNode()
    config.setJoin(join)
    config
  }

  override def parents: Seq[LogicalNode] = {
    val leftParents = utils.Config.from(join.left)
    val leftType = utils.TabularDataUtils.typeOf(join.left)

    val bootstrapParents =
      join.bootstrapParts
        .map(bp => utils.Config.from(bp.getTable, leftType))
        .toSeq

    val joinPartParents =
      join.joinParts
        .map(jp => GroupByNodeImpl(jp.groupBy).toConfig)
        .distinct

    val labelParents = Option(join.labelParts)
      .map(
        _.labels
          .map(jp => GroupByNodeImpl(jp.groupBy).toConfig)
          .distinct
      )
      .getOrElse(Seq.empty)

    leftParents +: (bootstrapParents ++ joinPartParents ++ labelParents)
  }

  override def semanticHash: String = {
    // function of everything except for metadata
    val baseJoin = join.deepCopy()

    baseJoin.unsetMetaData()

    // gb will be accounted through lineageHash later
    baseJoin.joinParts.foreach(_.unsetGroupBy())

    baseJoin.bootstrapParts.foreach(_.unsetMetaData())

    Option(baseJoin.labelParts).foreach(
      _.labels.foreach(_.unsetGroupBy())
    )

    baseJoin.hashCode().toHexString
  }

  override def tabularDataType: TabularDataType = TabularDataUtils.typeOf(join.left)
}
