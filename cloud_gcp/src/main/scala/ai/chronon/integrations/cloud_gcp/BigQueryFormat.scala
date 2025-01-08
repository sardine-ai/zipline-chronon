package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.Format
import ai.chronon.spark.FormatProvider
import ai.chronon.spark.Hive
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.ExternalTableDefinition
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConverters._

case class GcpFormatProvider(sparkSession: SparkSession) extends FormatProvider {
  // Order of Precedence for Default Project
  // Explicitly configured project in code (e.g., setProjectId()).
  // GOOGLE_CLOUD_PROJECT environment variable.
  // project_id from the ADC service account JSON file.
  // Active project in the gcloud CLI configuration.
  // No default project: An error will occur if no project ID is available.
  lazy val bqOptions = BigQueryOptions.getDefaultInstance
  lazy val bigQueryClient = bqOptions.getService

  override def resolveTableName(tableName: String): String = {
    format(tableName: String) match {
      case GCS(_, uri, _) => uri
      case _              => tableName
    }
  }

  override def readFormat(tableName: String): Format = format(tableName)

  // Fixed to BigQuery for now.
  override def writeFormat(tableName: String): Format = BQuery(bqOptions.getProjectId)

  private def format(tableName: String): Format = {

    val btTableIdentifier: TableId = BigQueryUtil.parseTableId(tableName)

    val tableOpt: Option[Table] = Option(
      bigQueryClient.getTable(btTableIdentifier.getDataset, btTableIdentifier.getTable))
    tableOpt
      .map((table) => {

        if (table.getDefinition.isInstanceOf[ExternalTableDefinition]) {
          val uris = table.getDefinition
            .asInstanceOf[ExternalTableDefinition]
            .getSourceUris
            .asScala
            .toList
            .map((uri) => uri.stripSuffix("/*") + "/")

          assert(uris.length == 1, s"External table ${tableName} can be backed by only one URI.")

          val formatStr = table.getDefinition
            .asInstanceOf[ExternalTableDefinition]
            .getFormatOptions
            .asInstanceOf[FormatOptions]
            .getType

          GCS(table.getTableId.getProject, uris.head, formatStr)
        } else if (table.getDefinition.isInstanceOf[StandardTableDefinition]) BQuery(table.getTableId.getProject)
        else throw new IllegalStateException(s"Cannot support table of type: ${table.getDefinition}")
      })
      .getOrElse(Hive)

    /**
      * Using federation
      * val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
      * val tableMeta = sparkSession.sessionState.catalog.getTableRawMetadata(tableIdentifier)
      * val storageProvider = tableMeta.provider
      * storageProvider match {
      * case Some("com.google.cloud.spark.bigquery") => {
      * val tableProperties = tableMeta.properties
      * val project = tableProperties
      * .get("FEDERATION_BIGQUERY_TABLE_PROPERTY")
      * .map(BigQueryUtil.parseTableId)
      * .map(_.getProject)
      * .getOrElse(throw new IllegalStateException("bigquery project required!"))
      * val bigQueryTableType = tableProperties.get("federation.bigquery.table.type")
      * bigQueryTableType.map(_.toUpperCase) match {
      * case Some("EXTERNAL") => GCS(project)
      * case Some("MANAGED")  => BQuery(project)
      * case None             => throw new IllegalStateException("Dataproc federation service must be available.")
      *
      * }
      *
      * case Some("hive") | None => Hive
      * }
      * */
  }
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

      // TODO: remove temporary hack. this is done because the existing raw data is in the date format yyyy-MM-dd
      //  but partition values in bigquery's INFORMATION_SCHEMA.PARTITIONS are in yyyyMMdd format.
      //  moving forward, for bigquery gcp we should default to storing raw data in yyyyMMdd format.
      val partitionFormat = sparkSession.conf.get("spark.chronon.partition.format", "yyyyMMdd")

      val partitionVals = sparkSession.read
        .format("bigquery")
        .option("project", project)
        .option("query", partValsSql)
        .load()
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
