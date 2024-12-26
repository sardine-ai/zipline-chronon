package ai.chronon.orchestration

import ai.chronon.orchestration.logical.GroupByNodeImpl
import ai.chronon.orchestration.logical.JoinNodeImpl
import ai.chronon.orchestration.logical.LogicalNodeImpl
import ai.chronon.orchestration.logical.ModelNodeImpl
import ai.chronon.orchestration.logical.StagingQueryNodeImpl
import ai.chronon.orchestration.logical.TabularDataNodeImpl
import ai.chronon.orchestration.utils.Config.getType

import scala.collection.mutable

object Builders {
  def nodeKey(name: String, logicalType: LogicalType, lineageHash: String = null): NodeKey = {
    val result = new NodeKey()
    result.setName(name)
    result.setLogicalType(logicalType)
    result.setLineageHash(lineageHash)
    result
  }
}

case class NodeInfo(inputConfigs: Seq[LogicalNode], config: LogicalNode, outputTables: Seq[String])

case class LineageIndex(tableToParent: mutable.Map[String, mutable.Buffer[NodeKey]] = mutable.HashMap.empty,
                        toChildConfig: mutable.Map[NodeKey, mutable.Buffer[NodeKey]] = mutable.HashMap.empty,
                        toInfo: mutable.Map[NodeKey, NodeInfo] = mutable.HashMap.empty) {
  // node, parent_nodes, child_nodes, input_tables, output_tables
}

object LineageIndex {

  def apply(logicalSet: LogicalSet): LineageIndex = {
    val index = LineageIndex()
    logicalSet.toLogicalNodes.foreach(update(_, index))
    index
  }

  def update(node: LogicalNodeImpl, index: LineageIndex): Unit = {

    val nodeKey = toKey(node)
    if (index.toInfo.contains(nodeKey)) return

    // update nodeInfo
    val parents = node.parents
    val info = NodeInfo(parents, node.toConfig, node.outputTables)
    index.toInfo.put(nodeKey, info)

    // register parents of tables
    node.outputTables.foreach { t =>
      val tabularNode = new TabularData()
      tabularNode.setTable(t)
      tabularNode.setType(node.tabularDataType)

      toKey(TabularDataNodeImpl(tabularNode))

      index.tableToParent
        .getOrElseUpdate(t, mutable.ArrayBuffer.empty)
        .append(nodeKey)
    }

    // register children of parents
    parents.foreach { parent =>
      val parentNode = configToNode(parent)
      val parentKey = toKey(parentNode)

      index.toChildConfig
        .getOrElseUpdate(parentKey, mutable.ArrayBuffer.empty)
        .append(nodeKey)

      update(parentNode, index)
    }
  }

  private def configToNode(config: LogicalNode): LogicalNodeImpl = {
    config match {
      case c if c.isSetJoin         => JoinNodeImpl(c.getJoin)
      case c if c.isSetGroupBy      => GroupByNodeImpl(c.getGroupBy)
      case c if c.isSetStagingQuery => StagingQueryNodeImpl(c.getStagingQuery)
      case c if c.isSetModel        => ModelNodeImpl(c.getModel)
      case c if c.isSetTabularData  => TabularDataNodeImpl(c.getTabularData)
    }
  }

  private def toKey(node: LogicalNodeImpl): NodeKey = {
    val nodeType = getType(node.toConfig)
    Builders.nodeKey(node.name, nodeType)
  }

}
