package ai.chronon.orchestration.logical

import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularData
import ai.chronon.orchestration.TabularDataType

case class TabularDataNodeImpl(tabularData: TabularData) extends LogicalNodeImpl {
  override def name: String = "tabular_data." + tabularData.table

  override def outputTables: Seq[String] =
    Seq.empty

  override def toConfig: LogicalNode = {
    val config = new LogicalNode()
    config.setTabularData(tabularData)
    config
  }

  override def parents: Seq[LogicalNode] =
    Seq.empty

  override def semanticHash: String = {
    tabularData.hashCode().toHexString
  }

  override def tabularDataType: TabularDataType = tabularData.getType
}
