package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.format.{Format, FormatProvider, Iceberg}
import com.google.cloud.bigquery._
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference
import org.apache.iceberg.exceptions.NoSuchIcebergTableException
import org.apache.iceberg.gcp.bigquery.{BigQueryClient, BigQueryClientImpl}
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._
import scala.util.Try

case class GcpFormatProvider(sparkSession: SparkSession) extends FormatProvider {

  /** Order of Precedence for Default Project:
    * - Explicitly configured project in code (e.g., setProjectId()).
    * - GOOGLE_CLOUD_PROJECT environment variable.
    * - project_id from the ADC service account JSON file.
    * - Active project in the gcloud CLI configuration.
    * - No default project: An error will occur if no project ID is available.
    */
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private lazy val bigQueryClient: BigQuery = bqOptions.getService
  private lazy val icebergClient: BigQueryClient = new BigQueryClientImpl()

  override def readFormat(tableName: String): scala.Option[Format] = {
    val btTableIdentifier = SparkBQUtils.toTableId(tableName)(sparkSession)
    scala.Option(bigQueryClient.getTable(btTableIdentifier)).map(getFormat)
  }

  private[cloud_gcp] def getFormat(table: Table): Format = {
    table.getDefinition.asInstanceOf[TableDefinition] match {
      case definition: ExternalTableDefinition =>
        Try {
          val tableRef = new TableReference()
            .setProjectId(table.getTableId.getProject)
            .setDatasetId(table.getTableId.getDataset)
            .setTableId(table.getTableId.getTable)

          icebergClient.getTable(tableRef) // Just try to load it. It'll fail if it's not an iceberg table.
          Iceberg
        }.recover {
          case canHandle: NoSuchIcebergTableException =>
            val formatOptions = definition.getFormatOptions.asInstanceOf[FormatOptions]

            val uri = scala
              .Option(definition.getHivePartitioningOptions)
              .map(_.getSourceUriPrefix)
              .getOrElse {
                val uris = definition.getSourceUris.asScala
                require(uris.size == 1, s"External table ${table} can be backed by only one URI.")
                uris.head.replaceAll("/\\*\\.parquet$", "")
              }

            GCS(uri, formatOptions.getType)
          case e: Exception => throw e
        }.get

      case _: StandardTableDefinition => BigQueryFormat

      case _ =>
        throw new IllegalStateException(s"Cannot support table of type: ${table.getFriendlyName}")
    }
  }
}
