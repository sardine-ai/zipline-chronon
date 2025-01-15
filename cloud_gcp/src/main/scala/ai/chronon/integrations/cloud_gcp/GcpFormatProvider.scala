package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.TableUtils
import ai.chronon.spark.format.Format
import ai.chronon.spark.format.FormatProvider
import ai.chronon.spark.format.Hive
import com.google.cloud.bigquery._
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId
import org.apache.spark.sql.SparkSession

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
    format(tableName) match {
      case GCS(_, uri, _) => uri
      case _              => tableName
    }

  override def readFormat(tableName: String): Format = format(tableName)

  override def writeFormat(table: String): Format = {

    val tu = TableUtils(sparkSession)

    val sparkOptions: Map[String, String] = Map(
      "partitionField" -> tu.partitionColumn,
      // todo(tchow): No longer needed after https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/1320
      "temporaryGcsBucket" -> sparkSession.conf.get("spark.chronon.table.gcs.temporary_gcs_bucket"),
      "writeMethod" -> "indirect"
    )

    BigQueryFormat(bqOptions.getProjectId, sparkOptions)
  }

  private def getFormat(table: Table): Format =
    table.getDefinition.asInstanceOf[TableDefinition] match {

      case definition: ExternalTableDefinition =>
        val uris = definition.getSourceUris.toScala
          .map(uri => uri.stripSuffix("/*") + "/")

        assert(uris.length == 1, s"External table ${table.getFriendlyName} can be backed by only one URI.")

        val formatOptions = definition.getFormatOptions
          .asInstanceOf[FormatOptions]

        GCS(table.getTableId.getProject, uris.head, formatOptions.getType)

      case _: StandardTableDefinition =>
        BigQueryFormat(table.getTableId.getProject, Map.empty)

      case _ => throw new IllegalStateException(s"Cannot support table of type: ${table.getFriendlyName}")
    }

  private def format(tableName: String): Format = {

    val btTableIdentifier: TableId = BigQueryUtil.parseTableId(tableName)

    val table = bigQueryClient.getTable(btTableIdentifier.getDataset, btTableIdentifier.getTable)

    // lookup bq for the table, if not fall back to hive
    scala.Option(table).map(getFormat).getOrElse(Hive)

  }
}
