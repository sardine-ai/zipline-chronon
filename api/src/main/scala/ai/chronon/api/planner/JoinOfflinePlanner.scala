package ai.chronon.api.planner

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps, StringOps}
import ai.chronon.api.ScalaJavaConversions.{IterableOps, IteratorOps}
import ai.chronon.api._
import ai.chronon.api.planner.JoinOfflinePlanner._
import ai.chronon.api.planner.GroupByOfflinePlanner._
import ai.chronon.orchestration._

import scala.collection.mutable
import scala.language.{implicitConversions, reflectiveCalls}

class JoinOfflinePlanner(join: Join)(implicit outputPartitionSpec: PartitionSpecWithColumn)
    extends Planner[Join](join)(outputPartitionSpec) {

  val leftSourceNode: SourceWithFilterNode = {
    val left = join.left
    val result = new SourceWithFilterNode()
      .setSource(left)
      .setExcludeKeys(join.skewKeys)

    val leftSourceHash = ThriftJsonCodec.hexDigest(result)
    val leftSourceTable = left.table.replace(".", "__").sanitize // source_namespace.table -> source_namespace__table
    val outputTableName =
      leftSourceTable + "__" + leftSourceHash + "/source_cache" // source__<source_namespace>__<table>__<hash>

    // at this point metaData.outputTable = join_namespace.source__<source_namespace>__<table>__<hash>
    val metaData = MetaDataUtils.layer(
      join.metaData,
      JoinNodeType.LEFT_SOURCE.toString.toLowerCase(),
      outputTableName,
      TableDependencies.fromSource(join.left).toSeq,
      stepDays = Some(1)
    )

    result.setMetaData(metaData)
  }

  val bootstrapNodeOpt: Option[JoinBootstrapNode] = Option(join.bootstrapParts).map { bootstrapParts =>
    val result = new JoinBootstrapNode()
      .setJoin(join)

    // bootstrap tables are unfortunately unique to the join - can't be re-used if a new join part is added
    val bootstrapNodeName = join.metaData.name + "/boostrap"

    val tableDeps = bootstrapParts.toScala.map { bp =>
      TableDependencies.fromTable(bp.table, bp.query)
    }.toSeq :+ TableDependencies.fromTable(leftSourceNode.metaData.outputTable)

    val metaData = MetaDataUtils.layer(
      join.metaData,
      JoinNodeType.BOOTSTRAP.toString.toLowerCase(),
      bootstrapNodeName,
      tableDeps,
      stepDays = Some(1)
    )

    result.setMetaData(metaData)
  }

  private def buildJoinPartNode(joinPart: JoinPart): JoinPartNode = {
    val result = new JoinPartNode()
      .setJoinPart(joinPart)
      .setLeftDataModel(join.left.dataModel)
      .setLeftSourceTable(leftSourceNode.metaData.outputTable)

    val partTable = RelevantLeftForJoinPart.partTableName(join, joinPart)

    val deps = TableDependencies.fromGroupBy(joinPart.groupBy, Option(join.left.dataModel)) :+
      TableDependencies.fromTable(leftSourceNode.metaData.outputTable)

    // use step days from group_by if set, otherwise default to 15d for events and 1 for entities
    val stepDays = Option(joinPart.groupBy.metaData.executionInfo)
      .filter(_.isSetStepDays)
      .map(_.stepDays)
      .getOrElse(joinPart.groupBy.dataModel match {
        case DataModel.ENTITIES => 1
        case DataModel.EVENTS   => 15
      })

    // pull conf params from the groupBy metadata, but use the join namespace to write to.
    val metaData = MetaDataUtils
      .layer(
        joinPart.groupBy.metaData,
        JoinNodeType.RIGHT_PART.toString.toLowerCase(),
        partTable,
        deps,
        stepDays = Some(stepDays)
      )
      .setOutputNamespace(join.metaData.outputNamespace)

    result.setMetaData(metaData)
  }

  private val joinPartNodes: Seq[JoinPartNode] = join.joinParts.toScala.map { buildJoinPartNode }.toSeq

  val mergeNode: JoinMergeNode = {
    val result = new JoinMergeNode()
      .setJoin(join)

    // sometimes the keys get bootstrapped. so we need to pick bootstraps if present for left side
    val leftTable = bootstrapNodeOpt
      .map(_.metaData.outputTable)
      .getOrElse(leftSourceNode.metaData.outputTable)

    // TODO: we might need to shift back 1 day for snapshot events case while partition sensing
    //
    // currently it works out fine, because we shift forward and back in the engine cancelling out the
    // date ranges that need to be scheduled
    val deps = joinPartNodes.map { jpNode =>
      TableDependencies.fromTable(jpNode.metaData.outputTable)
    } :+
      TableDependencies.fromTable(leftTable)

    val mergeNodeName = join.metaData.name + "/merged"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        JoinNodeType.MERGE.toString.toLowerCase(),
        mergeNodeName,
        deps,
        stepDays = Some(1)
      )

    result.setMetaData(metaData)
  }

  private val derivationNodeOpt: Option[JoinDerivationNode] = Option(join.derivations).map { _ =>
    val result = new JoinDerivationNode()
      .setJoin(join)

    val derivationNodeName = join.metaData.name + "/derived"

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        JoinNodeType.DERIVE.toString.toLowerCase(),
        derivationNodeName,
        Seq(TableDependencies.fromTable(mergeNode.metaData.outputTable)),
        stepDays = Some(1)
      )

    result.setMetaData(metaData)
  }

  // these need us to additionally (groupBy backfill) generate the snapshot tables
  private val snapshotLabelParts: Array[JoinPart] = Option(join.labelParts)
    .map(
      _.labels
        .iterator()
        .toScala
        .filter { jp => jp.groupBy.inferredAccuracy == Accuracy.SNAPSHOT }
        .toArray
    )
    .getOrElse(Array.empty)

  private val labelJoinNodeOpt: Option[LabelJoinNode] = Option(join.labelParts).map { labelParts =>
    val result = new LabelJoinNode()
      .setJoin(join)

    val labelNodeName = join.metaData.name + "/labeled"

    val inputTable = derivationNodeOpt
      .map(_.metaData.outputTable)
      .getOrElse(mergeNode.metaData.outputTable)

    val labelPartDeps = TableDependencies.fromJoin(join, labelParts) :+ TableDependencies.fromTable(inputTable)

    val metaData = MetaDataUtils
      .layer(
        join.metaData,
        JoinNodeType.LABEL_JOIN.toString.toLowerCase(),
        labelNodeName,
        labelPartDeps,
        stepDays = Some(1)
      )

    result.setMetaData(metaData)
  }

  override def offlineNodes: Seq[PlanNode] = {
    val result: mutable.ArrayBuffer[PlanNode] = mutable.ArrayBuffer.empty[PlanNode]

    result.append(leftSourceNode)
    bootstrapNodeOpt.foreach(bn => result.append(bn))
    joinPartNodes.foreach(jpn => result.append(jpn))

    result.append(mergeNode)
    derivationNodeOpt.foreach(dn => result.append(dn))
    snapshotLabelParts.foreach(lp => result.append(lp.groupBy))
    labelJoinNodeOpt.foreach(ljn => result.append(ljn))

    result
  }

  override def onlineNodes: Seq[PlanNode] = ???
}

object JoinOfflinePlanner {

  private def unsetNestedMetadata(join: Join): Unit = {
    join.unsetMetaData()
    Option(join.joinParts).foreach(_.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    Option(join.labelParts).foreach(_.labels.iterator().toScala.foreach(_.groupBy.unsetMetaData()))
    join.unsetOnlineExternalParts()
  }

  implicit class LabelJoinNodeIsPlanNode(node: LabelJoinNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      unsetNestedMetadata(result.join)
      result
    })
  }

  implicit class JoinDerivationNodeIsPlanNode(node: JoinDerivationNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      unsetNestedMetadata(result.join)
      result.join.unsetLabelParts()
      result
    })
  }

  implicit class JoinMergeNodeIsPlanNode(node: JoinMergeNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      unsetNestedMetadata(result.join)
      result.join.unsetDerivations()
      result.join.unsetLabelParts()
      result
    })
  }

  implicit class JoinPartNodeIsPlanNode(node: JoinPartNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      result.joinPart.groupBy.unsetMetaData()
      result
    })
  }

  implicit class JoinBootstrapNodeIsPlanNode(node: JoinBootstrapNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      unsetNestedMetadata(result.join)
      result
    })
  }

  implicit class SourceWithFilterNodeIsPlanNode(node: SourceWithFilterNode) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      result.unsetMetaData()
      result
    })
  }

  implicit class JoinIsPlanNode(node: Join) extends PlanNode {
    override def metaData: MetaData = node.metaData
    override def contents: Any = node
    override def semanticHash: String = ThriftJsonCodec.hexDigest({
      val result = node.deepCopy()
      unsetNestedMetadata(node)
      result
    })
  }

}
