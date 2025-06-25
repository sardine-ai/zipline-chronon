package ai.chronon.api.planner

import ai.chronon.api.{Join, PartitionSpec}
import ai.chronon.planner
import ai.chronon.planner.Node

import scala.collection.JavaConverters._

class MonolithJoinPlanner(join: Join)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Join](join)(outputPartitionSpec) {

  private def semanticMonolithJoin(join: Join): Join = {
    val semanticJoin = join.deepCopy()
    semanticJoin.unsetMetaData()
    Option(semanticJoin.joinParts).map(_.asScala).foreach { parts =>
      parts.foreach(joinPart => joinPart.groupBy.unsetMetaData())
    }
    Option(semanticJoin.bootstrapParts).map(_.asScala).foreach { bootstrapParts =>
      bootstrapParts.foreach(bootstrapPart => bootstrapPart.unsetMetaData())
    }
    Option(semanticJoin.labelParts).map(_.labels).map(_.asScala).foreach { labelPart =>
      labelPart.foreach { joinPart => joinPart.groupBy.unsetMetaData() }
    }
    semanticJoin.unsetOnlineExternalParts()
    semanticJoin

  }

  def backfillNode: Node = {
    val tableDeps = TableDependencies.fromJoin(join)

    val metaData =
      MetaDataUtils.layer(join.metaData, "backfill", join.metaData.name + "/backfill", tableDeps)
    val node = new planner.MonolithJoinNode().setJoin(join)
    toNode(metaData, _.setMonolithJoin(node), semanticMonolithJoin(join))
  }

  def metadataUploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload
    val metaData =
      MetaDataUtils.layer(join.metaData,
                          "metadata_upload",
                          join.metaData.name + "/metadata_upload",
                          Seq.empty,
                          Some(stepDays))
    val node = new planner.JoinMetadataUpload().setJoin(join)
    toNode(metaData, _.setJoinMetadataUpload(node), semanticMonolithJoin(join))
  }

  override def buildPlan: planner.ConfPlan = {
    val confPlan = new planner.ConfPlan()

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfillNode.metaData.name,
      planner.Mode.DEPLOY -> metadataUploadNode.metaData.name
    )

    confPlan.setNodes(List(backfillNode, metadataUploadNode).asJava).setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object MonolithJoinPlanner {
  def apply(join: Join)(implicit outputPartitionSpec: PartitionSpec): MonolithJoinPlanner = {
    new MonolithJoinPlanner(join)
  }
}
