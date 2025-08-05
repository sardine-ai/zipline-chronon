package ai.chronon.integrations.cloud_gcp

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.PartitionRange
import ai.chronon.spark.batch.StagingQuery
import ai.chronon.spark.catalog.{Format, TableUtils}
import scala.util.{Try, Failure, Success}
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  ExternalTableDefinition,
  FormatOptions,
  HivePartitioningOptions,
  JobInfo,
  QueryJobConfiguration,
  TableInfo
}

class BigQueryImport(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils)
    extends StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private[cloud_gcp] lazy val bigQueryClient: BigQuery = bqOptions.getService

  private val formatStr = "parquet"

  private[cloud_gcp] lazy val basePrefix = {
    val catalogName = Format.getCatalog(outputTable)(tableUtils.sparkSession)
    val warehouseLocation = tableUtils.sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
    s"${warehouseLocation}/export/${outputTable.sanitize}"
  }

  private[cloud_gcp] lazy val sourcePrefix = s"${basePrefix}/*.${formatStr}"

  private[cloud_gcp] def destPrefix(datePartitionColumn: String, datePartitionValue: String) =
    s"${basePrefix}/${datePartitionColumn}=${datePartitionValue}/*.${formatStr}"

  private[cloud_gcp] def exportDataTemplate(uri: String, sql: String): String = {

    // Requirements for the sql string:
    // `ds` cannot be part of the projection, it is reserved for chronon.
    // It can be part of the WHERE clause.
    f"""
       |EXPORT DATA
       |  OPTIONS (
       |    uri = '${uri}',
       |    format = '${formatStr}',
       |    overwrite = true
       |    )
       |AS (
       |   ${sql}
       |);
       |""".stripMargin
  }

  override def compute(range: PartitionRange, setups: Seq[String], enableAutoExpand: Option[Boolean]): Unit = {
    setups.foreach(tableUtils.sql)

    range.partitions.foreach { currPart =>
      val renderedQuery =
        StagingQuery.substitute(
          tableUtils,
          stagingQueryConf.query,
          currPart,
          currPart,
          endPartition
        )
      val destPath =
        destPrefix(range.partitionSpec.column, currPart)
      val exportTemplate =
        exportDataTemplate(destPath, renderedQuery)
      logger.info(s"Rendered Staging Query to run is:\n$exportTemplate")
      val exportConf = QueryJobConfiguration.of(exportTemplate)
      val exportJobTry = Try {
        val job = bigQueryClient.create(JobInfo.of(exportConf))
        job.waitFor()
      }.flatMap { job =>
        scala.Option(job.getStatus.getError) match {
          case Some(err) => Failure(new RuntimeException(s"Error exporting data to BigQuery ${err.getMessage}"))
          case None      => Success(job)
        }
      }

      // After exporting the data
      // Create the table in bigquery catalog if not exists. Not as iceberg.

      exportJobTry
        .map { _ =>
          val formatOptions = FormatOptions.of(formatStr.toUpperCase)
          val hivePartitioningOptions =
            HivePartitioningOptions
              .newBuilder()
              .setSourceUriPrefix(basePrefix)
              .setMode("STRINGS")
              .setRequirePartitionFilter(true)
              .build()
          val tableDefinition = ExternalTableDefinition
            .of(sourcePrefix, formatOptions)
            .toBuilder
            .setAutodetect(true)
            .setHivePartitioningOptions(hivePartitioningOptions)
            .build
          val tableInfo = TableInfo.of(SparkBQUtils.toTableId(outputTable)(tableUtils.sparkSession), tableDefinition)
          bigQueryClient.create(tableInfo)
        } match {
        case Success(table) =>
          logger.info(s"Wrote to table ${table.getTableId}, into partition: ${range.partitionSpec.column}=${currPart}")
        case Failure(exception) =>
          throw exception
      }
    }

    logger.info(s"Wrote to table $outputTable, into partitions: $range")
    logger.info(s"Finished writing Staging Query data to $outputTable")
  }

}
