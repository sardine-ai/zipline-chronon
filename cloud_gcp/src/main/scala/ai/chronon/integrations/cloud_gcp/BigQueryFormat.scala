package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.format.Format
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.spark.bigquery.SchemaConverters
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableInfo
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TimePartitioning
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date

case class BigQueryFormat(project: String, bqClient: BigQuery, override val options: Map[String, String])
    extends Format {
  override def name: String = "bigquery"

  override def alterTableProperties(tableName: String,
                                    tableProperties: Map[String, String]): (String => Unit) => Unit = {
    throw new NotImplementedError("alterTableProperties not yet supported for BigQuery")
  }

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): Seq[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)
  override def createTable(df: DataFrame,
                           tableName: String,
                           partitionColumns: Seq[String],
                           tableProperties: Map[String, String],
                           fileFormat: String): (String => Unit) => Unit = {

    def inner(df: DataFrame, tableName: String, partitionColumns: Seq[String])(sqlEvaluator: String => Unit) = {

      // See: https://cloud.google.com/bigquery/docs/partitioned-tables#limitations
      // "BigQuery does not support partitioning by multiple columns. Only one column can be used to partition a table."
      assert(partitionColumns.size < 2,
             s"BigQuery only supports at most one partition column, incoming spec: ${partitionColumns}")
      val shadedTableId = BigQueryUtil.parseTableId(tableName)

      val shadedBqSchema =
        SchemaConverters.from(SchemaConvertersConfiguration.createDefault()).toBigQuerySchema(df.schema)

      val baseTableDef = StandardTableDefinition.newBuilder
        .setSchema(shadedBqSchema)

      val tableDefinition = partitionColumns.headOption
        .map((col) => {
          val timePartitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField(col)
          baseTableDef
            .setTimePartitioning(timePartitioning.build())
        })
        .getOrElse(baseTableDef)

      val tableInfoBuilder = TableInfo.newBuilder(shadedTableId, tableDefinition.build)

      val tableInfo = tableInfoBuilder.build

      bqClient.create(tableInfo)
    }

    inner(df, tableName, partitionColumns)
  }

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    import sparkSession.implicits._
    val tableIdentifier = BigQueryUtil.parseTableId(tableName)
    val table = tableIdentifier.getTable
    val database =
      Option(tableIdentifier.getDataset).getOrElse(throw new IllegalArgumentException("database required!"))

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
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partColsSql)
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
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partValsSql)

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

  }

  def createTableTypeString: String =
    throw new UnsupportedOperationException("createTableTypeString not yet supported for BigQuery")
  def fileFormatString(format: String): String =
    throw new UnsupportedOperationException("fileFormatString not yet supported for BigQuery")

  override def supportSubPartitionsFilter: Boolean = true
}
