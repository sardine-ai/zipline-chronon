package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.TableUtils
import ai.chronon.spark.format.Format
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, to_date}

case object BigQueryNative extends Format {

  private val bqFormat = classOf[Spark35BigQueryTableProvider].getName
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance

  override def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {
    val bqTableId = SparkBQUtils.toTableId(tableName)
    val bqFriendlyName = scala.Option(bqTableId.getProject) match {
      case Some(project) => f"${project}.${bqTableId.getDataset}.${bqTableId.getTable}"
      case None          => f"${bqTableId.getDataset}.${bqTableId.getTable}"
    }
    val dfw = sparkSession.read.format(bqFormat)
    if (partitionFilters.isEmpty) {
      dfw.load(bqFriendlyName)
    } else {
      dfw
        .option("filter", partitionFilters)
        .load(bqFriendlyName)
    }
  }

  override def primaryPartitions(tableName: String, partitionColumn: String, subPartitionsFilter: Map[String, String])(
      implicit sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, subPartitionsFilter)

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): List[Map[String, String]] = {
    import sparkSession.implicits._
    val tableIdentifier = SparkBQUtils.toTableId(tableName)
    val providedProject = Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
    val table = tableIdentifier.getTable
    val database =
      Option(tableIdentifier.getDataset).getOrElse(
        throw new IllegalArgumentException(s"database required for table: ${tableName}"))

    // See: https://cloud.google.com/bigquery/docs/information-schema-columns
    val partColsSql =
      s"""
           |SELECT column_name FROM `${providedProject}.${database}.INFORMATION_SCHEMA.COLUMNS`
           |WHERE table_name = '${table}' AND is_partitioning_column = 'YES'
           |
           |""".stripMargin

    val partitionCol = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
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
           |SELECT partition_id FROM `${providedProject}.${database}.INFORMATION_SCHEMA.PARTITIONS`
           |WHERE table_name = '${table}'
           |
           |""".stripMargin

    // TODO: remove temporary hack. this is done because the existing raw data is in the date format yyyy-MM-dd
    //  but partition values in bigquery's INFORMATION_SCHEMA.PARTITIONS are in yyyyMMdd format.
    //  moving forward, for bigquery gcp we should default to storing raw data in yyyyMMdd format.
    val partitionFormat = TableUtils(sparkSession).partitionFormat

    val partitionInfoDf = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
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

  override def supportSubPartitionsFilter: Boolean = true
}
