package ai.chronon.hub.store

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import ai.chronon.hub.model.GroupBy
import ai.chronon.hub.model.Join
import ai.chronon.hub.model.Model
import ai.chronon.online.Api
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.ListRequest
import ai.chronon.online.KVStore.ListResponse
import ai.chronon.online.MetadataEndPoint
import ai.chronon.online.Metrics
import ai.chronon.online.TTLCache
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.collection.immutable.Seq

case class LoadedConfs(joins: Seq[api.Join] = Seq.empty,
                       groupBys: Seq[api.GroupBy] = Seq.empty,
                       stagingQueries: Seq[api.StagingQuery] = Seq.empty,
                       models: Seq[api.Model] = Seq.empty)

class MonitoringModelStore(apiImpl: Api) {

  val kvStore: KVStore = apiImpl.genKvStore
  implicit val executionContext: ExecutionContext = kvStore.executionContext

  // to help periodically refresh the load config catalog, we wrap this in a TTL cache
  lazy val configRegistryCache: TTLCache[String, LoadedConfs] = {
    new TTLCache[String, LoadedConfs](
      { _ =>
        Await.result(retrieveAllListConfs(LoadedConfs()), 10.seconds)
      },
      { _ => Metrics.Context(environment = "dynamodb_store.fetch_configs") }
    )
  }

  def getConfigRegistry: LoadedConfs = {
    configRegistryCache("default")
  }

  def getModels: Seq[Model] =
    configRegistryCache("default").models.flatMap { m =>
      if (m.source.isSetJoinSource && m.source.getJoinSource.isSetJoin) {
        val thriftJoin = m.source.getJoinSource.join

        val groupBys = thriftJoin.joinParts.asScala.map { part =>
          GroupBy(part.groupBy.metaData.name, part.groupBy.valueColumns)
        }

        val outputColumns = thriftJoin.outputColumnsByGroup.getOrElse("derivations", Array.empty)
        val join = Join(thriftJoin.metaData.name,
                        outputColumns,
                        groupBys,
                        thriftJoin.metaData.online,
                        thriftJoin.metaData.production,
                        Option(thriftJoin.metaData.team))
        Option(
          Model(m.metaData.name, join, m.metaData.online, m.metaData.production, m.metaData.team, m.modelType.name()))
      } else {
        logger.warn(s"Skipping model ${m.metaData.name} as it's missing join related details")
        None
      }
    }

  def getJoins: Seq[Join] = {
    configRegistryCache("default").joins.map { thriftJoin =>
      val groupBys = thriftJoin.joinParts.asScala.map { part =>
        GroupBy(part.groupBy.metaData.name, part.groupBy.valueColumns)
      }

      val outputColumns = thriftJoin.outputColumnsByGroup.getOrElse("derivations", Array.empty)
      Join(thriftJoin.metaData.name,
           outputColumns,
           groupBys,
           thriftJoin.metaData.online,
           thriftJoin.metaData.production,
           Option(thriftJoin.metaData.team))
    }
  }

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val defaultListLookupLimit: Int = 100

  private def retrieveAllListConfs(acc: LoadedConfs, paginationKey: Option[Any] = None): Future[LoadedConfs] = {
    val propsMap = {
      paginationKey match {
        case Some(key) => Map("limit" -> defaultListLookupLimit, "continuation-key" -> key)
        case None      => Map("limit" -> defaultListLookupLimit)
      }
    }
    val listRequest = ListRequest(MetadataEndPoint.ConfByKeyEndPointName, propsMap)
    logger.info(s"Triggering list conf lookup with request: $listRequest")
    kvStore.list(listRequest).flatMap { response =>
      val newLoadedConfs = makeLoadedConfs(response)
      val newAcc = LoadedConfs(
        acc.joins ++ newLoadedConfs.joins,
        acc.groupBys ++ newLoadedConfs.groupBys,
        acc.stagingQueries ++ newLoadedConfs.stagingQueries,
        acc.models ++ newLoadedConfs.models
      )
      if (response.resultProps.contains("continuation-key")) {
        retrieveAllListConfs(newAcc, response.resultProps.get("continuation-key"))
      } else {
        Future.successful(newAcc)
      }
    }
  }

  private def makeLoadedConfs(response: ListResponse): LoadedConfs = {
    response.values
      .map { seqAB =>
        val seqKVStrings = seqAB.map(kv =>
          (new String(kv.keyBytes, StandardCharsets.UTF_8), new String(kv.valueBytes, StandardCharsets.UTF_8)))
        val result = seqKVStrings.foldLeft(LoadedConfs()) { case (confs, kv) =>
          kv._1 match {
            case value if value.contains("joins/") =>
              LoadedConfs(confs.joins ++ Seq(getConf[api.Join](kv._2)),
                          confs.groupBys,
                          confs.stagingQueries,
                          confs.models)
            case value if value.contains("group_bys/") =>
              LoadedConfs(confs.joins,
                          confs.groupBys ++ Seq(getConf[api.GroupBy](kv._2)),
                          confs.stagingQueries,
                          confs.models)
            case value if value.contains("staging_queries/") =>
              LoadedConfs(confs.joins,
                          confs.groupBys,
                          confs.stagingQueries ++ Seq(getConf[api.StagingQuery](kv._2)),
                          confs.models)
            case value if value.contains("models/") =>
              LoadedConfs(confs.joins,
                          confs.groupBys,
                          confs.stagingQueries,
                          confs.models ++ Seq(getConf[api.Model](kv._2)))
            case _ =>
              logger.error(s"Unable to parse list response key: ${kv._1}")
              LoadedConfs()
          }
        }
        logger.info(
          s"Finished one batch load of configs. Loaded: ${result.joins.length} joins; " +
            s"${result.groupBys.length} GroupBys; ${result.models.length} models; " +
            s"${result.stagingQueries.length} staging queries")
        result
      }
      .recover { case e: Exception =>
        logger.error("Caught an exception", e)
        LoadedConfs(Seq.empty, Seq.empty, Seq.empty, Seq.empty)
      }
      .get
  }

  def getConf[T <: TBase[_, _]: Manifest](confString: String): T = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    ThriftJsonCodec.fromJsonStr[T](confString, false, clazz)
  }
}
