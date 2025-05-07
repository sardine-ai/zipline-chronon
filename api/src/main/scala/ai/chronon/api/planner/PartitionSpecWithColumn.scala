package ai.chronon.api.planner
import ai.chronon.api.PartitionSpec

case class PartitionSpecWithColumn(partitionColumn: String, partitionSpec: PartitionSpec)
