package ai.chronon.api.planner

import ai.chronon.api.Extensions.{MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api._
import ai.chronon.orchestration._

sealed trait Asset

case class Table(cluster: String, namespace: String, table: String)

case class TablePartitions(table: Table, partitionColumn: String, subPartitions: Option[String]) extends Asset

case class KvEntry(table: Table, keyBytes: Array[Byte]) extends Asset

case class Blob(path: String, partitionSuffix: String) extends Asset

case class AssetRange(asset: Asset, partitionRange: PartitionRange)

trait Conf {
  def json: String
  def metaData: MetaData

  // the last node should be the final output node
  def offlineNodes: Seq[Node]
  def onlineNodes: Seq[Node]
}

object Conf {
  def from(filePath: String): Conf = ???
}

sealed trait Node {
  def name: String
  def output: Asset
  def tableDependencies: List[TableDependency]
  def kvDependencies: List[KvDependency]

}

case class GroupByUpload(groupBy: GroupBy) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class GroupByBackfill(groupBy: GroupBy) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class GroupByStreaming(groupBy: GroupBy) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class SourceWithFilter(sourceWithFilterNode: SourceWithFilterNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class JoinBootstrap(joinBootstrapNode: JoinBootstrapNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class JoinPartJob(joinPartNode: JoinPartNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class JoinMerge(joinMergeNode: JoinMergeNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class JoinDerivation(joinDerivationNode: JoinDerivationNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class LabelPart(labelPartNode: LabelPartNode) extends Node {
  override def name: String = ???

  override def output: Asset = ???

  override def tableDependencies: List[TableDependency] = ???

  override def kvDependencies: List[KvDependency] = ???
}

case class JoinConf(join: Join) extends Conf {
  override def json: String = ThriftJsonCodec.toPrettyJsonStr(join)

  override def metaData: MetaData = join.metaData

  override def offlineNodes: Seq[Node] = {

    def metaDataCopy(): MetaData = join.metaData.deepCopy()
    def joinCopy(): Join = join.deepCopy()

    require(join.left != null, s"Left side of join ${join.metaData.getName} is null.")

    val leftSourceTableName = "TODO" // computeLeftSourceTableName(join)
    val sourceMetadata = metaDataCopy().setName(leftSourceTableName)
    val sourceNode = SourceWithFilter(
      new SourceWithFilterNode()
        .setSource(join.left)
        .setMetaData(sourceMetadata))

    require(join.metaData != null, s"Join metaData is null for join ${join.metaData.getName}.")

    val bootstrapNodeOpt = Option(join.bootstrapParts)
      .filterNot(_.isEmpty)
      .map { parts =>
        val result = new JoinBootstrapNode()
        result.setJoin(join)

        result.setMetaData(metaDataCopy().setName(join.metaData.getName + ".bootstrap"))
        JoinBootstrap(result)
      }

    val joinPartNodeOpt =
      for (
        parts <- Option(join.joinParts).toSeq;
        part <- parts.toScala
      ) yield {
        val partOutputTable = RelevantLeftForJoinPart.partTableName(join, part)
        val result = new JoinPartNode()
        result.setLeftSourceTable(sourceMetadata.outputTable)
        result.setJoinPart(part)
        val joinNodes =
          for (
            source <- part.groupBy.sources.toScala if source.isSetJoinSource;
            joinSource = source.getJoinSource
//            effectiveSource =
          ) yield {
//            val
//            JoinConf(joinSource.getJoin).offlineNodes
          }
        result.setLeftDataModel(join.left.dataModel)
        result.setMetaData(metaDataCopy().setName(partOutputTable))
        JoinPartJob(result)
      }

    val mergeNode = {
      val result = new JoinMergeNode()
      val joinCopyObj = joinCopy()
      joinCopyObj.unsetDerivations()
      joinCopyObj.unsetLabelParts() // remove join parts to avoid double counting
      result.setJoin(joinCopyObj)
      result.setMetaData(metaDataCopy().setName(join.metaData.getName + ".merge"))
      JoinMerge(result)
    }

    val derivationNode = {
      val result = new JoinDerivationNode()
      result.setJoin(join)
      result.setMetaData(metaDataCopy().setName(join.metaData.getName + ".derivations"))
      JoinDerivation(result)
    }

    val labelPartNodeOpt = Option(join.labelParts).map { labelParts =>
      val result = new LabelPartNode()
      result.setJoin(join)
      result.setMetaData(metaDataCopy().setName(join.metaData.getName + ".label_parts"))
      LabelPart(result)
    }

    (bootstrapNodeOpt ++
      Seq(sourceNode) ++
      joinPartNodeOpt ++
      Seq(mergeNode) ++
      Seq(derivationNode) ++
      labelPartNodeOpt).toSeq
  }

  override def onlineNodes: Seq[Node] = ???
}

case class StagingQueryConf(stagingQuery: StagingQuery) extends Conf {
  override def json: String = ThriftJsonCodec.toPrettyJsonStr(stagingQuery)

  override def metaData: MetaData = stagingQuery.metaData

  override def offlineNodes: List[Node] = ???

  override def onlineNodes: Seq[Node] = ???
}

case class GroupByConf(groupBy: GroupBy) extends Conf {
  override def json: String = ThriftJsonCodec.toPrettyJsonStr(groupBy)

  override def metaData: MetaData = groupBy.metaData

  override def offlineNodes: List[Node] = ???

  override def onlineNodes: Seq[Node] = ???
}

case class ModelConf(model: Model) extends Conf {
  override def json: String = ThriftJsonCodec.toPrettyJsonStr(model)

  override def metaData: MetaData = model.metaData

  override def offlineNodes: List[Node] = ???

  override def onlineNodes: Seq[Node] = ???
}

trait Dependency {}

object Dependency {}

object ConfToNodes {}
