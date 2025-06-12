package ai.chronon.api.planner

import ai.chronon.api.thrift.TBase
import ai.chronon.api.{MetaData, PartitionSpec, ThriftJsonCodec}
import ai.chronon.planner.{ConfPlan, Node, NodeContent}

import scala.collection.Seq

case class Mode(name: String, nodes: Seq[Node], cron: String)

abstract class Planner[T](conf: T)(implicit outputPartitionSpec: PartitionSpec) {

  def buildPlan: ConfPlan

  def toNode[T <: TBase[_, _]: Manifest](metaData: MetaData,
                                         contentSetter: NodeContent => Unit,
                                         hashableNode: T): Node = {
    val hash = ThriftJsonCodec.hexDigest(hashableNode)

    val content = new NodeContent()
    contentSetter(content)

    new Node()
      .setContent(content)
      .setMetaData(metaData)
      .setSemanticHash(hash)
  }
}
