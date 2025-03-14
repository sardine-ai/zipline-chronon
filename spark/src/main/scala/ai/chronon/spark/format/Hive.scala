package ai.chronon.spark.format

import org.apache.spark.sql.SparkSession

case object Hive extends Format {

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): List[Map[String, String]] = {
    // data is structured as a Df with single composite partition key column. Every row is a partition with the
    // column values filled out as a formatted key=value pair
    // Eg. df schema = (partitions: String)
    // rows = [ "day=2020-10-10/hour=00", ... ]
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map(row => super.parseHivePartition(row.getString(0)))
      .toList
  }

  override def supportSubPartitionsFilter: Boolean = true
}
