package ai.chronon.integrations.cloud_gcp
import ai.chronon.spark.format.{DefaultFormatProvider, Format, Iceberg}
import com.google.cloud.bigquery._
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog

import scala.jdk.CollectionConverters._
import scala.util.Try

class GcpFormatProvider(override val sparkSession: SparkSession) extends DefaultFormatProvider(sparkSession) {

  /** Order of Precedence for Default Project:
    * - Explicitly configured project in code (e.g., setProjectId()).
    * - GOOGLE_CLOUD_PROJECT environment variable.
    * - project_id from the ADC service account JSON file.
    * - Active project in the gcloud CLI configuration.
    * - No default project: An error will occur if no project ID is available.
    */

  override def readFormat(tableName: String): scala.Option[Format] = {
    val parsedCatalog = getCatalog(tableName)
    val identifier = SparkBQUtils.toIdentifier(tableName)(sparkSession)
    val cat = sparkSession.sessionState.catalogManager.catalog(parsedCatalog)
    cat match {
      case delegating: DelegatingBigQueryMetastoreCatalog =>
        Try {
          delegating
            .loadTable(identifier)
            .properties
            .asScala
            .getOrElse(TableCatalog.PROP_PROVIDER, "")
            .toUpperCase match {
            case "ICEBERG"   => Iceberg
            case "BIGQUERY"  => BigQueryNative
            case "PARQUET"   => BigQueryExternal
            case unsupported => throw new IllegalStateException(s"Unsupported provider type: ${unsupported}")
          }
        }.toOption
      case iceberg: SparkCatalog if (iceberg.icebergCatalog().isInstanceOf[BigQueryMetastoreCatalog]) =>
        scala.Option(Iceberg)
      case _ => super.readFormat(tableName)
    }
  }
}
