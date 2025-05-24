package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions._
import ai.chronon.spark.ingest.DataImport
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Job, JobInfo, QueryJobConfiguration, TableId}
import com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.api.PartitionRange
import ai.chronon.spark.catalog.Format
import scala.util.{Failure, Success, Try}
import java.util.UUID

class BigQueryImport extends DataImport {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private lazy val bigQueryClient: BigQuery = bqOptions.getService
  private val internalBQPartitionCol = "__chronon_internal_bq_partition_col__"
  private val formatStr = "parquet"
  private val bqFormat = classOf[Spark35BigQueryTableProvider].getName

  private def exportDataTemplate(uri: String, format: String, sql: String): String =
    f"""
       |EXPORT DATA
       |  OPTIONS (
       |    uri = '${uri}',
       |    format = '${format}',
       |    overwrite = false
       |    )
       |AS (
       |   ${sql}
       |);
       |""".stripMargin

  private def getPartitionColumn(projectId: String, bqTableId: TableId)(implicit
      sparkSession: SparkSession): scala.Option[NativePartColumn] = {
    import sparkSession.implicits._
    val partColsSql =
      s"""
         |SELECT column_name, is_system_defined FROM `${projectId}.${bqTableId.getDataset}.INFORMATION_SCHEMA.COLUMNS`
         |WHERE table_name = '${bqTableId.getTable}' AND is_partitioning_column = 'YES'
         |
         |""".stripMargin

    val pColOption = sparkSession.read
      .format(bqFormat)
      .option("project", projectId)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", bqTableId.getDataset)
      .load(partColsSql)
      .as[(String, String)]
      .collect
      .map { case (name, isSystemDefined) =>
        NativePartColumn(
          name,
          isSystemDefined = isSystemDefined match {
            case "YES" => true
            case "NO"  => false
            case _ =>
              throw new IllegalArgumentException(s"Unknown partition column system definition: ${isSystemDefined}")
          }
        )
      }
      .headOption

    pColOption
  }

  private[cloud_gcp] def destPrefix(catalogName: String,
                                    tableName: String,
                                    formatStr: String,
                                    uniqueId: scala.Option[String] = None)(implicit sparkSession: SparkSession) = {
    val warehouseLocation = sparkSession.sessionState.conf
      .getConfString(s"spark.sql.catalog.${catalogName}.warehouse")
      .stripSuffix("/")
    val uuid = uniqueId.getOrElse(UUID.randomUUID().toString)
    s"${warehouseLocation}/export/${tableName.sanitize}_${uuid}/*.${formatStr}"

  }

  override def sync(sourceTableName: String, destinationTableName: String, partitionRange: PartitionRange)(implicit
      sparkSession: SparkSession): Unit = {
    // First, need to clean the spark-based table name for the bigquery queries below.
    val bqTableId = SparkBQUtils.toTableId(sourceTableName)
    val providedProject = scala.Option(bqTableId.getProject).getOrElse(bqOptions.getProjectId)
    val bqFriendlyName = f"${providedProject}.${bqTableId.getDataset}.${bqTableId.getTable}"

    // Next, check to see if the table has a pseudo partition column
    val pColOption = getPartitionColumn(providedProject, bqTableId)
    val spec = partitionRange.partitionSpec
    val whereClauses = Format.andPredicates(partitionRange.whereClauses)
    val partitionWheres = if (whereClauses.nonEmpty) s"WHERE ${whereClauses}" else whereClauses
    val select = pColOption match {
      case Some(nativeCol) => {
        require(
          nativeCol.colName.equals(spec.column),
          s"Configured column in the pipeline definition ${spec.column} does not match what's defined on the BigQuery table: ${nativeCol.colName}. "
        )
        if (nativeCol.isSystemDefined)
          s"SELECT ${nativeCol.colName} as ${internalBQPartitionCol}, * FROM ${bqFriendlyName} ${partitionWheres}"
        else
          s"SELECT * FROM ${bqFriendlyName} ${partitionWheres}"
      }
      case _ => s"SELECT * FROM ${bqFriendlyName} ${partitionWheres}"
    }
    val catalogName = Format.getCatalog(sourceTableName)
    val destPath = destPrefix(
      catalogName = catalogName,
      tableName = destinationTableName,
      formatStr = formatStr
    )

    val exportSQL = exportDataTemplate(
      uri = destPath,
      format = formatStr,
      sql = select
    )

    logger.info(s"Starting BigQuery export job for table: ${bqFriendlyName}")
    val exportJobTry: Try[Job] = Try {
      val exportConf = QueryJobConfiguration.of(exportSQL)
      val job = bigQueryClient.create(JobInfo.of(exportConf))
      scala.Option(job.waitFor()).getOrElse(throw new RuntimeException("Export job returned null"))
    }

    exportJobTry.flatMap { job =>
      scala.Option(job.getStatus.getError) match {
        case Some(err) => Failure(new RuntimeException(s"BigQuery export job failed: $err"))
        case None      => Success(job)
      }
    } match {
      case Failure(e) =>
        throw e
      case Success(_) =>
        val internalLoad = sparkSession.read.format(formatStr).load(destPath)
        val tableUtils = TableUtils(sparkSession)
        val dfToWrite = pColOption
          .map { case (nativeColumn) => // as long as we have a native partition column we'll attempt to rename it.
            internalLoad
              .withColumnRenamed(internalBQPartitionCol, nativeColumn.colName)
          }
          .getOrElse(internalLoad)
        tableUtils
          .insertPartitions(dfToWrite, destinationTableName, partitionColumns = List(spec.column))
    }
  }
}
