package ai.chronon.orchestration.logical

import ai.chronon.orchestration.LogicalNode
import ai.chronon.orchestration.TabularDataType

// Base trait for common node operations
trait LogicalNodeImpl {
  // unique name for the node
  def name: String
  def outputTables: Seq[String]
  def toConfig: LogicalNode
  def parents: Seq[LogicalNode]
  def semanticHash: String
  def tabularDataType: TabularDataType
}
