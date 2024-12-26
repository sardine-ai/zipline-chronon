package ai.chronon.orchestration.logical

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Model
import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularDataType
import ai.chronon.orchestration.utils
import ai.chronon.orchestration.utils.TabularDataUtils

// Model implementation
case class ModelNodeImpl(model: Model) extends LogicalNodeImpl {
  override def name: String = model.metaData.name

  override def outputTables: Seq[String] =
    Seq(model.metaData.outputTable)

  override def toConfig: LogicalNode = {
    val config = new LogicalNode()
    config.setModel(model)
    config
  }

  override def parents: Seq[LogicalNode] = Seq(utils.Config.from(model.source))

  override def semanticHash: String = {
    model.deepCopy().unsetMetaData().hashCode().toHexString
  }

  override def tabularDataType: TabularDataType = TabularDataUtils.typeOf(model.source)
}
