package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.GroupBy
import ai.chronon.api.MetaData
import ai.chronon.api.PartitionSpec
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.KVStore.ListValue
import ai.chronon.online.Metrics
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryErrorMessages
import com.google.cloud.bigquery.BigQueryRetryConfig
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.admin.v2.models.GCRules
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Filters
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.cloud.bigtable.data.v2.models.{TableId => BTTableId}
import com.google.protobuf.ByteString
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.threeten.bp.Duration

import java.nio.charset.Charset
import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

/**
  * BigTable based KV store implementation. We store a few kinds of data in our KV store:
  * 1) Entity data - An example is thrift serialized Groupby / Join configs. If entities are updated / rewritten, we
  * serve the latest version.
  * 2) Timeseries data - This is either our batch IRs or streaming tiles for feature fetching. It also
  * includes drift / skew time summaries.
  *
  * We have multi use-case tables for the _BATCH and _STREAMING time series tile data.
  * To ensure that data from different groupBys are isolated from each other, we prefix the key with the dataset name:
  * Row key: dataset#key
  *
  * In case of time series data that is likely to see many data points per day (e.g. tile_summaries, streaming tiles), we
  * bucket the data by day to ensure that we don't need to filter a Row with thousands of cells (and also worry about the per Row size / cell count limits).
  * This also helps as GC in BigTable can take ~1 week. Without this day based bucketing we might have cells spanning a week.
  *
  * This row key structure looks like (tile size included in case of streaming tiles to support tile layering):
  * Row key: dataset#key#timestamp_rounded_to_day[#tileSize]
  *
  * Values are written to individual cells with timestamp of the time series point being the cell timestamp.
  *
  * Tables created via this client have a default TTL of 5 days and a max cell count of 10k. This is to ensure we don't
  * store data indefinitely and also to cap the amount of data we store.
  */
class BigTableKVStoreImpl(dataClient: BigtableDataClient,
                          adminClient: BigtableTableAdminClient,
                          maybeBigQueryClient: Option[BigQuery] = None)
    extends KVStore {

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import BigTableKVStore._

  // We keep data around for a 5 day TTL. This gives us a little buffer in case of incidents while still capping our storage
  private val DataTTL = Duration.ofDays(5)

  // Cap the maximum number of cells we store.
  private val MaxCellCount = 10000

  // BT docs (https://cloud.google.com/bigtable/docs/garbage-collection) cover this more - union ensures we GC data if either rule is met
  private val DefaultGcRules =
    GCRules.GCRULES.union().rule(GCRules.GCRULES.maxAge(DataTTL)).rule(GCRules.GCRULES.maxVersions(MaxCellCount))

  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("bigtable")

  override def create(dataset: String): Unit = create(dataset, Map.empty)

  override def create(dataset: String, props: Map[String, Any]): Unit = {

    try {

      if (!adminClient.exists(dataset)) {

        // we can explore split points if we need custom tablet partitioning. For now though, we leave this to BT
        val createTableRequest = CreateTableRequest.of(dataset).addFamily(ColumnFamilyString, DefaultGcRules)
        val table = adminClient.createTable(createTableRequest)
        // TODO: this actually submits an async task. thus, the submission can succeed but the task can fail.
        //  doesn't return a future but maybe we can poll
        logger.info(s"Created table: $table")
        metricsContext.increment("create.successes")

      } else {

        logger.info(s"Table $dataset already exists")

      }
    } catch {

      case e: Exception =>
        logger.error("Error creating table", e)
        metricsContext.increment("create.failures", s"exception:${e.getClass.getName}")

    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    logger.info(s"Performing multi-get for ${requests.size} requests")
    val resultFutures = requests.map { request =>
      val query = Query
        .create(mapDatasetToTable(request.dataset))
        .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
        .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))

      val tableType = getTableType(request.dataset)

      (request.startTsMillis, tableType) match {

        case (Some(startTs), TileSummaries) =>
          val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())
          setQueryTimeSeriesFilters(query, startTs, endTime, request.keyBytes, request.dataset)

        case (Some(startTs), StreamingTable) =>
          // we generate the RowKey corresponding to the given tile details (identified by
          // dataset, keybytes and tile size). The time range required (startTs to endTime) is
          // used to query the appropriate set of tiles
          val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
          val tileSizeMs = tileKey.tileSizeMillis
          val baseKeyBytes = tileKey.keyBytes.asScala.map(_.asInstanceOf[Byte])
          val endTime = request.endTsMillis.getOrElse(System.currentTimeMillis())
          setQueryTimeSeriesFilters(query, startTs, endTime, baseKeyBytes, request.dataset, Some(tileSizeMs))

        case _ =>
          // for non-timeseries data, we just look up based on the base row key
          val baseRowKey = buildRowKey(request.keyBytes, request.dataset)
          query.rowKey(ByteString.copyFrom(baseRowKey))
          // we also limit to the latest cell per row as we don't want clients to iterate over all prior edits
          query.filter(Filters.FILTERS.limit().cellsPerRow(1))
      }

      val startTs = System.currentTimeMillis()
      // we go through a couple of future conversion hops to go from ApiFuture to Scala Future
      val apiFuture = dataClient.readRowsCallable().all().futureCall(query)
      val completableFuture = ApiFutureUtils.toCompletableFuture(apiFuture)
      val scalaResultFuture = FutureConverters.toScala(completableFuture)

      scalaResultFuture
        .map { rows =>
          metricsContext.distribution("multiGet.latency", System.currentTimeMillis() - startTs)
          metricsContext.increment("multiGet.successes")

          val timedValues = rows.asScala.flatMap { row =>
            row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
              // Convert back to milliseconds
              KVStore.TimedValue(cell.getValue.toByteArray, cell.getTimestamp / 1000)
            }
          }

          KVStore.GetResponse(request, Success(timedValues))

        }
        .recover {
          case e: Exception =>
            logger.error("Error getting values", e)
            metricsContext.increment("multiGet.bigtable_errors", s"exception:${e.getClass.getName}")
            KVStore.GetResponse(request, Failure(e))
        }
    }

    Future.sequence(resultFutures)
  }

  private def setQueryTimeSeriesFilters(query: Query,
                                        startTs: Long,
                                        endTs: Long,
                                        keyBytes: Seq[Byte],
                                        dataset: String,
                                        maybeTileSize: Option[Long] = None): Query = {
    // we need to generate a rowkey corresponding to each day from the startTs to now
    val millisPerDay = 1.day.toMillis

    val startDay = startTs - (startTs % millisPerDay)
    val endDay = endTs - (endTs % millisPerDay)

    (startDay to endDay by millisPerDay).foreach(dayTs => {
      val rowKey =
        maybeTileSize
          .map(tileSize => buildTiledRowKey(keyBytes, dataset, dayTs, tileSize))
          .getOrElse(buildRowKey(keyBytes, dataset, Some(dayTs)))

      query.rowKey(ByteString.copyFrom(rowKey))
    })

    // Bigtable uses microseconds and we need to scan from startTs (millis) to endTs (millis)
    query.filter(Filters.FILTERS.timestamp().range().startClosed(startTs * 1000).endClosed(endTs * 1000))
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.info(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get(BigTableKVStore.listLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => defaultListLimit
    }

    val maybeStartKey = request.props.get(continuationKey)

    val query = Query
      .create(mapDatasetToTable(request.dataset))
      .filter(Filters.FILTERS.family().exactMatch(ColumnFamilyString))
      .filter(Filters.FILTERS.qualifier().exactMatch(ColumnFamilyQualifierString))
      // we also limit to the latest cell per row as we don't want clients to iterate over all prior edits
      .filter(Filters.FILTERS.limit().cellsPerRow(1))
      .limit(listLimit)

    // if we got a start row key, lets wire it up
    maybeStartKey.foreach { startKey =>
      query.range(ByteStringRange.unbounded().startOpen(ByteString.copyFrom(startKey.asInstanceOf[Array[Byte]])))
    }

    val startTs = System.currentTimeMillis()
    val rowsApiFuture = dataClient.readRowsCallable().all.futureCall(query)

    val rowCompletableFuture = ApiFutureUtils.toCompletableFuture(rowsApiFuture)
    val rowsScalaFuture = FutureConverters.toScala(rowCompletableFuture)

    rowsScalaFuture
      .map { rows =>
        metricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        metricsContext.increment("list.successes")

        val listValues = rows.asScala.flatMap { row =>
          row.getCells(ColumnFamilyString, ColumnFamilyQualifier).asScala.map { cell =>
            ListValue(row.getKey.toByteArray, cell.getValue.toByteArray)
          }
        }

        val propsMap: Map[String, Any] =
          if (listValues.size < listLimit) {
            Map.empty // last page, we're done
          } else
            Map(continuationKey -> listValues.last.keyBytes)

        ListResponse(request, Success(listValues), propsMap)

      }
      .recover {

        case e: Exception =>
          logger.error("Error listing values", e)
          metricsContext.increment("list.bigtable_errors", s"exception:${e.getClass.getName}")

          ListResponse(request, Failure(e), Map.empty)

      }
  }

  // We stick to individual put calls here as our invocations are fairly small sized sequences (couple of elements).
  // Using the individual mutate calls allows us to easily return fine-grained success/failure information in the form
  // our callers expect.
  override def multiPut(requests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} requests")
    val resultFutures = {
      requests.map { request =>
        val tableId = mapDatasetToTable(request.dataset)

        val tableType = getTableType(request.dataset)
        val timestampInPutRequest = request.tsMillis.getOrElse(System.currentTimeMillis())

        val (rowKey, timestamp) = (request.tsMillis, tableType) match {
          case (Some(ts), TileSummaries) =>
            (buildRowKey(request.keyBytes, request.dataset, Some(ts)), timestampInPutRequest)
          case (Some(ts), StreamingTable) =>
            val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
            val baseKeyBytes = tileKey.keyBytes.asScala.map(_.asInstanceOf[Byte])
            (buildTiledRowKey(baseKeyBytes, request.dataset, ts, tileKey.tileSizeMillis),
             tileKey.tileStartTimestampMillis)
          case _ =>
            (buildRowKey(request.keyBytes, request.dataset), timestampInPutRequest)
        }

        val timestampMicros = timestamp * 1000
        val mutation = RowMutation.create(tableId, ByteString.copyFrom(rowKey))
        val cellValue = ByteString.copyFrom(request.valueBytes)
        // if we have prior cells with the same timestamp, we queue up a delete operation before the put
        mutation.deleteCells(ColumnFamilyString,
                             ColumnFamilyQualifier,
                             TimestampRange.create(timestampMicros, timestampMicros + 1000))
        mutation.setCell(ColumnFamilyString, ColumnFamilyQualifier, timestampMicros, cellValue)

        val startTs = System.currentTimeMillis()
        val mutateApiFuture = dataClient.mutateRowAsync(mutation)
        val completableFuture = ApiFutureUtils.toCompletableFuture(mutateApiFuture)
        val scalaFuture = FutureConverters.toScala(completableFuture)
        scalaFuture
          .map { _ =>
            metricsContext.distribution("multiPut.latency", System.currentTimeMillis() - startTs)
            metricsContext.increment("multiPut.successes")
            true
          }
          .recover {
            case e: Exception =>
              logger.error("Error putting data", e)
              metricsContext.increment("multiPut.failures", s"exception:${e.getClass.getName}")
              false
          }
      }
    }
    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    if (maybeBigQueryClient.isEmpty) {
      logger.error("BigQuery client not available, cannot export data to BigTable")
      metricsContext.increment("bulkPut.failures", "exception:missingbigqueryclient")
      throw new RuntimeException("BigQuery client not available, cannot export data to BigTable")
    }

    // we write groupby data to 1 large multi use-case table
    val batchTable = "GROUPBY_BATCH"

    // we use the endDs + span to indicate the timestamp of all the cell data we upload for endDs
    // this is used in the KV store multiget calls
    val partitionSpec = PartitionSpec("yyyy-MM-dd", WindowUtils.Day.millis)
    val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

    // we need to sanitize and append the batch suffix to the groupBy name as that's
    // what we use to look things up while fetching
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(destinationOnlineDataSet))
    val datasetName = groupBy.batchDataset

    val exportQuery =
      s"""
         |EXPORT DATA OPTIONS (
         |  format='CLOUD_BIGTABLE',
         |  overwrite=true,
         |  uri="https://bigtable.googleapis.com/projects/${adminClient.getProjectId}/instances/${adminClient.getInstanceId}/appProfiles/GROUPBY_INGEST/tables/$batchTable",
         |  bigtable_options='''{
         |   "columnFamilies" : [
         |      {
         |        "familyId": "cf",
         |        "encoding": "BINARY",
         |        "columns": [
         |           {"qualifierString": "value", "fieldName": ""}
         |        ]
         |      }
         |   ]
         |}'''
         |) AS
         |SELECT
         |  CONCAT(CAST(CONCAT('$datasetName', '#') AS BYTES), key_bytes) as rowkey,
         |  value_bytes as cf,
         |  TIMESTAMP_MILLIS($endDsPlusOne) as _CHANGE_TIMESTAMP
         |FROM $sourceOfflineTable
         |WHERE ds = '$partition'
         |""".stripMargin
    logger.info(s"Kicking off bulkLoad with query:\n$exportQuery")

    maybeBigQueryClient.foreach { bigQueryClient =>
      val queryConfig = QueryJobConfiguration
        .newBuilder(exportQuery)
        .build()

      val startTs = System.currentTimeMillis()
      // we append the timestamp to the jobID as BigQuery doesn't allow us to re-run the same job
      val jobId =
        JobId.of(adminClient.getProjectId, s"export_${sourceOfflineTable.sanitize}_to_bigtable_${partition}_$startTs")
      val job: Job = bigQueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
      logger.info(s"Export job started with Id: $jobId and link: ${job.getSelfLink}")
      val retryConfig =
        BigQueryRetryConfig.newBuilder
          .retryOnMessage(BigQueryErrorMessages.RATE_LIMIT_EXCEEDED_MSG)
          .retryOnMessage(BigQueryErrorMessages.JOB_RATE_LIMIT_EXCEEDED_MSG)
          .build

      val initialRetryDelay = Duration.ofMinutes(1)
      val totalRetryTimeout = Duration.ofHours(6)
      logger.info(s"We will wait for $totalRetryTimeout for the job to complete")
      val completedJob = job.waitFor(retryConfig,
                                     RetryOption.initialRetryDelay(initialRetryDelay),
                                     RetryOption.totalTimeout(totalRetryTimeout))
      if (completedJob == null) {
        // job no longer exists
        logger.error(s"Job corresponding to $jobId no longer exists")
        metricsContext.increment("bulkPut.failures", "exception:missingjob")
        throw new RuntimeException(s"Export job corresponding to $jobId no longer exists")
      } else if (completedJob.getStatus.getError != null) {
        logger.error(s"Job failed with error: ${completedJob.getStatus.getError}")
        metricsContext.increment("bulkPut.failures", s"exception:${completedJob.getStatus.getError.getReason}")
        throw new RuntimeException(s"Export job failed with error: ${completedJob.getStatus.getError}")
      } else {
        logger.info("Export job completed successfully")
        metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
        metricsContext.increment("bulkPut.successes")
      }
    }
  }
}

object BigTableKVStore {

  // continuation key to help with list pagination
  val continuationKey: String = "continuationKey"

  // Limit of max number of entries to return in a list call
  val listLimit: String = "limit"

  // Default list limit
  val defaultListLimit: Int = 100

  sealed trait TableType
  case object BatchTable extends TableType
  case object StreamingTable extends TableType
  case object TileSummaries extends TableType

  /**
    * row key (with tiling) convention:
    * <dataset>#<entity-key>#<start_date>#<tile_size>
    *
    *  row key (without tiling) convention:
    *  <dataset>#<entity_key>#<start_date>
    */
  def buildTiledRowKey(baseKeyBytes: Seq[Byte], dataset: String, ts: Long, tileSizeMs: Long): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    val dayTs = ts - (ts % 1.day.toMillis)
    baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8")) ++ s"#$tileSizeMs".getBytes(Charset.forName("UTF-8"))
  }

  // We prefix the dataset name to the key to ensure we can have multiple datasets in the same table
  def buildRowKey(baseKeyBytes: Seq[Byte], dataset: String, maybeTs: Option[Long] = None): Array[Byte] = {
    val baseRowKey = s"$dataset#".getBytes(Charset.forName("UTF-8")) ++ baseKeyBytes
    maybeTs match {
      case Some(ts) =>
        // For time series data, we append the day timestamp to the row key to ensure that time series points across different
        // days are split across rows
        val dayTs = ts - (ts % 1.day.toMillis)
        baseRowKey ++ s"#$dayTs".getBytes(Charset.forName("UTF-8"))
      case _ => baseRowKey
    }
  }

  def mapDatasetToTable(dataset: String): BTTableId = {
    if (dataset.endsWith("_BATCH")) {
      BTTableId.of("GROUPBY_BATCH")
    } else if (dataset.endsWith("_STREAMING")) {
      BTTableId.of("GROUPBY_STREAMING")
    } else {
      BTTableId.of(dataset)
    }
  }

  def getTableType(dataset: String): TableType = {
    dataset match {
      case d if d.endsWith("_BATCH")     => BatchTable
      case d if d.endsWith("_STREAMING") => StreamingTable
      case d if d.endsWith("SUMMARIES")  => TileSummaries
      case _                             => BatchTable // default to batch table for tables like chronon_metadata
    }
  }

  val ColumnFamilyString: String = "cf"
  val ColumnFamilyQualifierString: String = "value"
  val ColumnFamilyQualifier: ByteString = ByteString.copyFromUtf8(ColumnFamilyQualifierString)
}
