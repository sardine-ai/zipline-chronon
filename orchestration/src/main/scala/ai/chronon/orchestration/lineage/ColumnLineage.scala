package ai.chronon.orchestration.lineage


sealed trait ColumnLineageNode
case class QualifiedColumn(table: String, column: String) extends ColumnLineageNode
case class Transformation(inputs: Seq[QualifiedColumn], outputColumn: Option[QualifiedColumn], expressions: Seq[String])
// made this up, but we need to effectively match this
case class ColumnLineage(inputs: Seq[QualifiedColumn], outputColumn: String, expression: String, filters: String)

class ColumnLineageExtractor {

  def from(query: String): Array[]
}
