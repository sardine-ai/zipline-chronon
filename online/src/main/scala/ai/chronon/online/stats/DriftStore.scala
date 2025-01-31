package ai.chronon.online.stats

import ai.chronon.api
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api._
import ai.chronon.api.thrift.TDeserializer
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.protocol.TBinaryProtocol
import ai.chronon.api.thrift.protocol.TProtocolFactory
import ai.chronon.observability.DriftMetric
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileKey
import ai.chronon.observability.TileSeriesKey
import ai.chronon.observability.TileSummary
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.MetadataStore
import ai.chronon.online.stats.DriftStore.binaryDeserializer
import ai.chronon.online.stats.DriftStore.binarySerializer

import java.io.Serializable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class DriftStore(kvStore: KVStore,
                 summaryDataset: String = Constants.TiledSummaryDataset,
                 metadataDataset: String = Constants.MetadataDataset)
    extends MetadataStore(kvStore = kvStore, dataset = metadataDataset, timeoutMillis = 1000L) {

  def tileKeysForJoin(join: api.Join,
                      slice: Option[String] = None,
                      columnNamePrefix: Option[String] = None): Map[String, Array[TileKey]] = {
    val joinName = join.getMetaData.getName
    val tileSize = join.getMetaData.driftTileSize

    require(tileSize.nonEmpty, s"Drift tile size not set for join $joinName")

    val tileSizeMillis = tileSize.get.millis

    // output columns by groupBy
    val outputValueColumnsMap = join.outputColumnsByGroup

    outputValueColumnsMap.mapValues {
      _.filter { col => columnNamePrefix.forall(col.startsWith) }
        .map { column =>
          val key = new TileKey()
          slice.foreach(key.setSlice)
          key.setName(joinName)
          key.setColumn(column)
          key.setSizeMillis(tileSizeMillis)
          key
        }
    }
  }

  private case class SummaryRequestContext(request: GetRequest, tileKey: TileKey, groupName: String)
  private case class SummaryResponseContext(summaries: Array[(TileSummary, Long)], tileKey: TileKey, groupName: String)

  case class TileSummaryInfo(key: TileSeriesKey, summaries: Array[(TileSummary, Long)]) {
    def toDriftSeries(driftMetric: DriftMetric, lookBack: Window, startMs: Long): TileDriftSeries = {
      val driftsArray = TileDriftCalculator.toTileDrifts(summaries, driftMetric, startMs, lookBack)
      val result = PivotUtils.pivot(driftsArray)
      result.setKey(key)
    }

    def toSeries: TileSummarySeries = {
      val result = PivotUtils.pivot(summaries)
      result.setKey(key)
    }
  }

  // scatter gathers via a single multi-get
  def getSummaries(joinConf: api.Join,
                   startMs: Option[Long],
                   endMs: Option[Long],
                   columnPrefix: Option[String]): Future[Seq[TileSummaryInfo]] = {

    val serializer: TSerializer = binarySerializer.get()
    val tileKeyMap = tileKeysForJoin(joinConf, None, columnPrefix)
    val requestContextMap: Map[GetRequest, SummaryRequestContext] = tileKeyMap.flatMap {
      case (group, keys) =>
        // Only create requests for keys that match our column prefix
        keys
          .filter(key => columnPrefix.forall(prefix => key.getColumn == prefix))
          .map { key =>
            val keyBytes = serializer.serialize(key)
            val get = GetRequest(keyBytes, summaryDataset, startTsMillis = startMs, endTsMillis = endMs)
            get -> SummaryRequestContext(get, key, group)
          }
    }

    val responseFuture = kvStore.multiGet(requestContextMap.keys.toSeq)

    responseFuture.map { responses =>
      val deserializer = binaryDeserializer.get()
      // deserialize the responses and surround with context
      val responseContextTries: Seq[Try[SummaryResponseContext]] = responses.map { response =>
        val valuesTry = response.values
        val request = response.request
        val requestContext = requestContextMap(request)
        val tileKey = requestContext.tileKey
        val groupName = requestContext.groupName
        valuesTry.map { values =>
          val summaries =
            if (values == null)
              null
            else
              values.map { value =>
                val summary = new TileSummary()
                deserializer.deserialize(summary, value.bytes)
                summary -> value.millis
              }.toArray

          SummaryResponseContext(summaries, tileKey, groupName)
        }
      }

      // handle failures
      val responseContexts: Seq[SummaryResponseContext] = responseContextTries.flatMap {
        _ match {
          case Success(responseContext) => Some(responseContext)
          // TODO instrument failures
          case Failure(exception) => exception.printStackTrace(); None
        }
      }

      responseContexts.map { responseContext =>
        val tileSeriesKey = new TileSeriesKey()
        tileSeriesKey.setSlice(responseContext.tileKey.getSlice)
        tileSeriesKey.setNodeName(joinConf.getMetaData.nameToFilePath)
        tileSeriesKey.setGroupName(responseContext.groupName)
        tileSeriesKey.setColumn(responseContext.tileKey.getColumn)

        TileSummaryInfo(tileSeriesKey, responseContext.summaries)
      }
    }
  }

  case class Range(startMs: Long, endMs: Long) {
    // if lookBack is too large and the range is too small, we will make separate queries
    // e.g. if we have a range of 1 day and lookBack of 1 week, we will make two queries (8 days ago, 7 days ago) + (1 day ago, today)
    // if the range is too large and the lookBack is too small, we will make single query
    // e.g. if we have a range of 1 week and lookBack of 1 day, we will make single query - (8 days ago, today)
    def lookBack(lookBackMs: Long): (Range, Option[Range]) = {
      if (endMs - lookBackMs >= startMs) { // single query
        Range(startMs - lookBackMs, endMs) -> None
      } else {
        Range(startMs - lookBackMs, endMs - lookBackMs) -> Some(Range(startMs, endMs))
      }
    }
  }

  private def getSummariesForRange(join: api.Join,
                                   range: Range,
                                   lookBack: Long,
                                   columnPrefix: Option[String] = None): Future[Seq[TileSummaryInfo]] = {
    val (currentRange, oldRangeOpt) = range.lookBack(lookBack)
    val currentSummaries = getSummaries(join, Some(currentRange.startMs), Some(currentRange.endMs), columnPrefix)
    if (oldRangeOpt.isEmpty) {
      currentSummaries
    } else {
      val oldRange = oldRangeOpt.get
      val oldSummaries = getSummaries(join, Some(oldRange.startMs), Some(oldRange.endMs), columnPrefix)
      Future.sequence(Seq(currentSummaries, oldSummaries)).map {
        case Seq(current, old) =>
          old ++ current
      }
    }
  }

  def getDriftSeries(join: String,
                     driftMetric: DriftMetric,
                     lookBack: Window,
                     startMs: Long,
                     endMs: Long,
                     columnPrefix: Option[String] = None): Try[Future[Seq[TileDriftSeries]]] = {
    getJoinConf(join).map { joinConf =>
      // TODO-explore: we might be over fetching if lookBack is much larger than end - start
      getSummariesForRange(joinConf.join, Range(startMs, endMs), lookBack.millis, columnPrefix).map {
        tileSummaryInfos =>
          tileSummaryInfos.map { tileSummaryInfo =>
            tileSummaryInfo.toDriftSeries(driftMetric, lookBack, startMs)
          }
      }
    }
  }

  def getSummarySeries(join: String,
                       startMs: Long,
                       endMs: Long,
                       columnPrefix: Option[String] = None): Try[Future[Seq[TileSummarySeries]]] = {
    getJoinConf(join).map { joinConf =>
      getSummaries(joinConf.join, Some(startMs), Some(endMs), columnPrefix).map { tileSummaryInfos =>
        tileSummaryInfos.map { tileSummaryInfo =>
          tileSummaryInfo.toSeries
        }
      }
    }
  }
}

object DriftStore {
  class SerializableSerializer(factory: TProtocolFactory) extends TSerializer(factory) with Serializable

  // crazy bug in compact protocol - do not change to compact

  @transient
  lazy val binarySerializer: ThreadLocal[TSerializer] = new ThreadLocal[TSerializer] {
    override def initialValue(): TSerializer = new TSerializer(new TBinaryProtocol.Factory())
  }

  @transient
  lazy val binaryDeserializer: ThreadLocal[TDeserializer] = new ThreadLocal[TDeserializer] {
    override def initialValue(): TDeserializer = new TDeserializer(new TBinaryProtocol.Factory())
  }

  def breaks(count: Int): Seq[String] = (0 to count).map(_ * (100 / count)).map("p" + _.toString)
}
