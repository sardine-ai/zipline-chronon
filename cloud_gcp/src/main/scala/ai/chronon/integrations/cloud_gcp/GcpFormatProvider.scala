package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.format.{DefaultFormatProvider, Format, Iceberg}
import com.google.cloud.bigquery._
import com.google.cloud.iceberg.bigquery.relocated.com.google.api.services.bigquery.model.TableReference
import org.apache.iceberg.exceptions.NoSuchIcebergTableException
import org.apache.iceberg.gcp.bigquery.{BigQueryClient, BigQueryClientImpl}
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class GcpFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

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
    logger.info(s"Retrieving read format for table: ${tableName}")

    // order is important here. we want the Hive case where we just check for table in catalog to be last
    Try {
      val btTableIdentifier = SparkBQUtils.toTableId(tableName)(sparkSession)
      val bqTable = bigQueryClient.getTable(btTableIdentifier)
      getFormat(bqTable)
    } match {
      case Success(format) => scala.Option(format)
      case Failure(e) =>
        logger.info(s"${tableName} is not a BigQuery table")
        super.readFormat(tableName)
    }
  }

  private[cloud_gcp] def getFormat(table: Table): Format = {
    table.getDefinition.asInstanceOf[TableDefinition] match {
      case _: ExternalTableDefinition =>
        Try {
          val tableRef = new TableReference()
            .setProjectId(table.getTableId.getProject)
            .setDatasetId(table.getTableId.getDataset)
            .setTableId(table.getTableId.getTable)

          icebergClient.getTable(tableRef) // Just try to load it. It'll fail if it's not an iceberg table.
          Iceberg
        }.recover {
          case _: NoSuchIcebergTableException => BigQueryExternal
          case e: Exception                   => throw e
        }.get

      case _: StandardTableDefinition => BigQueryNative

      case _ =>
        throw new IllegalStateException(s"Cannot support table of type: ${table.getFriendlyName}")
    }
  }
}
