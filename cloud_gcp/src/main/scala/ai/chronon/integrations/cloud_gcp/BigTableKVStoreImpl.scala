package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.KVStore
import com.google.cloud.bigquery._
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.models.Filters
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.cloud.bigtable.data.v2.models.{TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

class BigTableKVStoreImpl(projectId: String, instanceId: String) extends KVStore {
  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val dataClient: BigtableDataClient = {
    val settings = BigtableDataSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
      .build()
    BigtableDataClient.create(settings)
  }

  override def create(dataset: String): Unit = {
    logger.info(s"Creating dataset: $dataset")
    // Implementation would depend on how you want to handle table creation
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] =
    Future {
      logger.info(s"Performing multi-get for ${requests.size} requests")
      requests.map { request =>
        val rowKey = ByteString.copyFrom(request.keyBytes)
        val query = Query
          .create(BTTableId.of(request.dataset))
          .rowKey(rowKey)
          .filter(Filters.FILTERS.family().exactMatch(columnFamilyString))
          .filter(Filters.FILTERS.qualifier().exactMatch(columnFamilyQualifierString))

        val queryTime = System.currentTimeMillis()
        // scan from afterTsMillis to now - skip events with future timestamps
        request.startTsMillis.foreach { ts =>
          // Bigtable uses microseconds
          query.filter(Filters.FILTERS.timestamp().range().startOpen(ts * 1000).endClosed(queryTime))
        }

        try {
          val rows = dataClient.readRows(query).iterator().asScala.toSeq
          val timedValues = rows.flatMap { row =>
            row.getCells(columnFamilyString, columnFamilyQualifier).asScala.map { cell =>
              // Convert back to milliseconds
              KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
            }
          }
          KVStore.GetResponse(request, Success(timedValues))
        } catch {
          case e: Exception =>
            logger.error(s"Error getting values: ${e.getMessage}")
            KVStore.GetResponse(request, Failure(e))
        }
      }
    }

  private val columnFamilyString: String = "cf"
  private val columnFamilyQualifierString: String = "value"
  private val columnFamilyQualifier: ByteString = ByteString.copyFromUtf8(columnFamilyQualifierString)
  // TODO figure out if we are actually writing partitioned data (I think we are not but we should)
  private val partitionColumn: String = "ds"

  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] =
    Future {
      logger.info(s"Performing multi-put for ${requests.size} requests")
      requests.map { request =>
        val tableId = BTTableId.of(request.dataset)
        val mutation = RowMutation.create(tableId, ByteString.copyFrom(request.keyBytes))
        val timestamp = request.tsMillis.getOrElse(System.currentTimeMillis())
        val cellValue = ByteString.copyFrom(request.valueBytes)
        mutation.setCell(columnFamilyString, columnFamilyQualifier, timestamp * 1000, cellValue)

        try {
          dataClient.mutateRow(mutation)
          true
        } catch {
          case e: Exception =>
            logger.error(s"Error putting key-value pair: ${e.getMessage}")
            false
        }
      }
    }

  private val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    logger.info(
      s"Performing bulk put from BigQuery table $sourceOfflineTable to Bigtable dataset $destinationOnlineDataSet for partition $partition")

    val exportQuery = s"""
      EXPORT DATA OPTIONS(
        uri='https://bigtable.googleapis.com/projects/$projectId/instances/$instanceId/tables/$destinationOnlineDataSet',
        format='CLOUD_BIGTABLE',
        bigtable_options='''
        {
          "columnFamilies": [
            {
              "familyId": "$columnFamilyString",
              "columns": [
                {
                  "qualifierString": "$columnFamilyQualifierString",
                  "fieldName": "value"
                }
              ]
            }
          ]
        }
        '''
      ) AS
      SELECT
        key as rowkey,
        value
      FROM `$sourceOfflineTable`
      WHERE $partitionColumn='$partition'
    """

    val queryConfig = QueryJobConfiguration
      .newBuilder(exportQuery)
      .setUseLegacySql(false)
      .build()

    val jobId = JobId.of(projectId, s"export_${sourceOfflineTable}_to_bigtable_${System.currentTimeMillis()}")
    val job: Job = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

    logger.info(s"Export job started: ${job.getSelfLink}")
    // Wait for the job to complete
    job.waitFor()

    if (job.getStatus.getError != null) {
      logger.error(s"Export job failed: ${job.getStatus.getError}")
      throw new RuntimeException(s"Export job failed: ${job.getStatus.getError}")
    } else {
      logger.info("Export job completed successfully")
    }
  }
}
