package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.format.Format
import ai.chronon.spark.format.FormatProvider
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.ExternalTableDefinition
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.FormatOptions
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.JobInfo
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Table
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableDefinition
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

case class GcpFormatProvider(sparkSession: SparkSession) extends FormatProvider {

  /**
    * Order of Precedence for Default Project:
    * - Explicitly configured project in code (e.g., setProjectId()).
    * - GOOGLE_CLOUD_PROJECT environment variable.
    * - project_id from the ADC service account JSON file.
    * - Active project in the gcloud CLI configuration.
    * - No default project: An error will occur if no project ID is available.
    */
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def resolveTableName(tableName: String): String =
    format(tableName)
      .map {
        case GCS(uri, _)             => uri
        case BigQueryFormat(_, _, _) => tableName
        case _                       => tableName
      }
      .getOrElse(tableName)

  override def readFormat(tableName: String): Option[Format] = format(tableName)

  override def writeFormat(table: String): Format = {
    val tableId = BigQueryUtil.parseTableId(table)
    assert(scala.Option(tableId.getProject).isDefined, s"project required for ${table}")
    assert(scala.Option(tableId.getDataset).isDefined, s"dataset required for ${table}")

    val sparkOptions: Map[String, String] = Map(
      "writeMethod" -> "direct",
      "createDisposition" -> JobInfo.CreateDisposition.CREATE_NEVER.name
    )

    BigQueryFormat(tableId.getProject, bigQueryClient, sparkOptions)
  }

  private[cloud_gcp] def getFormat(table: Table): Format =
    table.getDefinition.asInstanceOf[TableDefinition] match {

      case definition: ExternalTableDefinition =>
        val formatOptions = definition.getFormatOptions
          .asInstanceOf[FormatOptions]
        val externalTable = table.getDefinition.asInstanceOf[ExternalTableDefinition]
        val uri = Option(externalTable.getHivePartitioningOptions)
          .map(_.getSourceUriPrefix)
          .getOrElse {
            val uris = externalTable.getSourceUris.asScala
            require(uris.size == 1, s"External table ${table} can be backed by only one URI.")
            uris.head.replaceAll("/\\*\\.parquet$", "")
          }

        GCS(uri, formatOptions.getType)

      case _: StandardTableDefinition =>
        BigQueryFormat(table.getTableId.getProject, bigQueryClient, Map.empty)

      case _ => throw new IllegalStateException(s"Cannot support table of type: ${table.getFriendlyName}")
    }

  private def format(tableName: String): Option[Format] = {

    val btTableIdentifier: TableId = BigQueryUtil.parseTableId(tableName)
    val table = Option(bigQueryClient.getTable(btTableIdentifier.getDataset, btTableIdentifier.getTable))
    table
      .map(getFormat)

  }
}
