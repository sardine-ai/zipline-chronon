package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.format.Format
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date

case class BigQueryFormat(project: String, override val options: Map[String, String]) extends Format {

  override def name: String = "bigquery"

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    import sparkSession.implicits._
    val tableIdentifier = BigQueryUtil.parseTableId(tableName)
    val table = tableIdentifier.getTable
    val database =
      Option(tableIdentifier.getDataset).getOrElse(throw new IllegalArgumentException("database required!"))

    val originalViewsEnabled = sparkSession.conf.get("viewsEnabled", false.toString)
    val originalMaterializationDataset = sparkSession.conf.get("materializationDataset", "")

    // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
    // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations

    sparkSession.conf.set("viewsEnabled", true)
    sparkSession.conf.set("materializationDataset", database)

    try {

      // See: https://cloud.google.com/bigquery/docs/information-schema-columns
      val partColsSql =
        s"""
           |SELECT column_name FROM `${project}.${database}.INFORMATION_SCHEMA.COLUMNS`
           |WHERE table_name = '${table}' AND is_partitioning_column = 'YES'
           |
           |""".stripMargin

      val partitionCol = sparkSession.read
        .format("bigquery")
        .option("project", project)
        .option("query", partColsSql)
        .load()
        .as[String]
        .collect
        .headOption
        .getOrElse(throw new UnsupportedOperationException(s"No partition column for table ${tableName} found."))

      // See: https://cloud.google.com/bigquery/docs/information-schema-partitions
      val partValsSql =
        s"""
           |SELECT partition_id FROM `${project}.${database}.INFORMATION_SCHEMA.PARTITIONS`
           |WHERE table_name = '${table}'
           |
           |""".stripMargin

      // TODO: remove temporary hack. this is done because the existing raw data is in the date format yyyy-MM-dd
      //  but partition values in bigquery's INFORMATION_SCHEMA.PARTITIONS are in yyyyMMdd format.
      //  moving forward, for bigquery gcp we should default to storing raw data in yyyyMMdd format.
      val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyyMMdd")

      val partitionInfoDf = sparkSession.read
        .format("bigquery")
        .option("project", project)
        .option("query", partValsSql)
        .load()

      val partitionVals = partitionInfoDf
        .select(
          date_format(
            to_date(
              col("partition_id"),
              "yyyyMMdd" // Note: this "yyyyMMdd" format is hardcoded but we need to change it to be something else.
            ),
            partitionFormat)
            .as("partition_id"))
        .na // Should filter out '__NULL__' and '__UNPARTITIONED__'. See: https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables
        .drop()
        .as[String]
        .collect
        .toList

      partitionVals.map((p) => Map(partitionCol -> p))

    } finally {

      sparkSession.conf.set("viewsEnabled", originalViewsEnabled)
      sparkSession.conf.set("materializationDataset", originalMaterializationDataset)

    }

  }

  def createTableTypeString: String = "BIGQUERY"
  def fileFormatString(format: String): String = ""

  override def supportSubPartitionsFilter: Boolean = true
}
