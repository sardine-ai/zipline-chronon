package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.Format
import ai.chronon.spark.FormatProvider
import ai.chronon.spark.Hive
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.ExternalTableDefinition
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.bigquery.{TableId => BTableId}
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId
import org.apache.spark.sql.SparkSession

case class GcpFormatProvider(sparkSession: SparkSession) extends FormatProvider {

  lazy val bigQueryClient = BigQueryOptions.getDefaultInstance.getService
  def readFormat(tableName: String): Format = {

    val btTableIdentifier: TableId = BigQueryUtil.parseTableId(tableName)
    val unshadedTI: BTableId =
      BTableId.of(btTableIdentifier.getProject, btTableIdentifier.getDataset, btTableIdentifier.getTable)

    val tableOpt = Option(bigQueryClient.getTable(unshadedTI))

    tableOpt match {
      case Some(table) => {
        table.getDefinition match {
          case _: ExternalTableDefinition => BQuery(unshadedTI.getProject)
          case _: StandardTableDefinition => GCS(unshadedTI.getProject)
        }
      }
      case None => Hive
    }

    /**
    Using federation
     val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
     val tableMeta = sparkSession.sessionState.catalog.getTableRawMetadata(tableIdentifier)
    val storageProvider = tableMeta.provider
    storageProvider match {
      case Some("com.google.cloud.spark.bigquery") => {
        val tableProperties = tableMeta.properties
        val project = tableProperties
          .get("FEDERATION_BIGQUERY_TABLE_PROPERTY")
          .map(BigQueryUtil.parseTableId)
          .map(_.getProject)
          .getOrElse(throw new IllegalStateException("bigquery project required!"))
        val bigQueryTableType = tableProperties.get("federation.bigquery.table.type")
        bigQueryTableType.map(_.toUpperCase) match {
          case Some("EXTERNAL") => GCS(project)
          case Some("MANAGED")  => BQuery(project)
          case None             => throw new IllegalStateException("Dataproc federation service must be available.")

        }
      }

      case Some("hive") | None => Hive
    }
      * */

  }

  // For now, fix to BigQuery. We'll clean this up.
  def writeFormat(tableName: String): Format = ???
}

case class BQuery(project: String) extends Format {

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

      val partitionVals = sparkSession.read
        .format("bigquery")
        .option("project", project)
        .option("query", partValsSql)
        .load()
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
