package ai.chronon.spark.format

import org.apache.spark.sql.SparkSession

case object Hive extends Format {

  override def name: String = "hive"

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  private def parseHivePartition(pstring: String): Map[String, String] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toMap
  }

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): List[Map[String, String]] = {
    // data is structured as a Df with single composite partition key column. Every row is a partition with the
    // column values filled out as a formatted key=value pair
    // Eg. df schema = (partitions: String)
    // rows = [ "day=2020-10-10/hour=00", ... ]
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map(row => parseHivePartition(row.getString(0)))
      .toList
  }

  def createTableTypeString: String = ""

  def fileFormatString(format: String): String = s"STORED AS $format"

  override def supportSubPartitionsFilter: Boolean = true
}
