/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.fetcher

import ai.chronon.api.Constants._
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.api._
import ai.chronon.api.thrift.TBase
import ai.chronon.online.KVStore.{ListRequest, ListResponse, PutRequest}
import ai.chronon.online.MetadataEndPoint.NameByTeamEndPointName
import ai.chronon.online.OnlineDerivationUtil.buildDerivedFields
import ai.chronon.online._
import ai.chronon.online.serde.AvroCodec
import ai.chronon.online.metrics.{Metrics, TTLCache}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import scala.collection.immutable.SortedMap
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// [timestamp -> {metric name -> metric value}]
case class DataMetrics(series: Seq[(Long, SortedMap[String, Any])])

case class ConfPathOrName(confPath: Option[String] = None, confName: Option[String] = None) {

  if (confPath.isEmpty && confName.isEmpty) {
    throw new IllegalArgumentException("confPath and confName cannot be both empty")
  }

  def computeConfKey(confKeyword: String): String = {
    if (confName.isDefined) {
      s"$confKeyword/" + confName.get

    } else {
      s"$confKeyword/" + confPath.get.split("/").takeRight(1).head
    }
  }
}

class MetadataStore(fetchContext: FetchContext) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private var partitionSpec =
    PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  private val CONF_BATCH_SIZE = 50

  // Note this should match with the format used in the warehouse
  def setPartitionMeta(format: String, spanMillis: Long): Unit = {
    partitionSpec = PartitionSpec(format = format, spanMillis = spanMillis)
  }

  // Note this should match with the format used in the warehouse
  def setPartitionMeta(format: String): Unit = {
    partitionSpec = PartitionSpec(format = format, spanMillis = partitionSpec.spanMillis)
  }

  implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  def getConf[T <: TBase[_, _]: Manifest](confPathOrName: ConfPathOrName): Try[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

    val confTypeKeyword = clazz match {
      case j if j == classOf[Join]           => JoinKeyword
      case g if g == classOf[GroupBy]        => GroupByKeyword
      case sq if sq == classOf[StagingQuery] => StagingQueryKeyword
      case m if m == classOf[Model]          => ModelKeyword
      case _                                 => throw new IllegalArgumentException(s"Unsupported conf type: $clazz")
    }

    val confKey = confPathOrName.computeConfKey(confTypeKeyword)
    fetchContext.kvStore
      .getString(confKey, fetchContext.metadataDataset, fetchContext.timeoutMillis)
      .map(conf => ThriftJsonCodec.fromJsonStr[T](conf, false, clazz))
      .recoverWith { case th: Throwable =>
        Failure(
          new RuntimeException(
            s"Couldn't fetch ${clazz.getName} for key $confKey. Perhaps metadata upload wasn't successful.",
            th
          ))
      }
  }

  private def getEntityListByTeam[T <: TBase[_, _]: Manifest](team: String): Try[Seq[String]] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val dataset = NameByTeamEndPointName
    fetchContext.kvStore
      .getStringArray(team, dataset, fetchContext.timeoutMillis)
      .recoverWith { case th: Throwable =>
        Failure(
          new RuntimeException(
            s"Couldn't fetch ${clazz.getName} for key $team. Perhaps metadata upload wasn't successful.",
            th
          ))
      }
  }

  lazy val getGroupByListByTeam: TTLCache[String, Try[Seq[String]]] = {
    new TTLCache[String, Try[Seq[String]]](
      { team =>
        getEntityListByTeam[GroupBy]("group_bys/" + team)
          .recover { case e: java.util.NoSuchElementException =>
            logger.error(
              s"Failed to fetch conf for team $team at group_bys/$team, please check metadata upload to make sure the metadata has been uploaded")
            throw e
          }
      },
      { team =>
        Metrics.Context(environment = "group_by.list.fetch", groupBy = team)
      }
    )
  }

  lazy val getJoinListByTeam: TTLCache[String, Try[Seq[String]]] = {
    new TTLCache[String, Try[Seq[String]]](
      { team =>
        getEntityListByTeam[Join]("joins/" + team)
          .recover { case e: java.util.NoSuchElementException =>
            logger.error(
              s"Failed to fetch conf for team $team at joins/$team, please check metadata upload to make sure the metadata has been uploaded")
            throw e
          }
      },
      { team =>
        import ai.chronon.online.metrics
        metrics.Metrics.Context(environment = "join.list.fetch", groupBy = team)
      }
    )
  }

  lazy val getJoinConf: TTLCache[String, Try[JoinOps]] = new TTLCache[String, Try[JoinOps]](
    { name =>
      import ai.chronon.online.metrics
      val startTimeMs = System.currentTimeMillis()
      val result = getConf[Join](ConfPathOrName(confName = Some(name)))
        .recover { case e: java.util.NoSuchElementException =>
          logger.error(
            s"Failed to fetch conf for join $name at joins/$name, please check metadata upload to make sure the join metadata for $name has been uploaded")
          throw e
        }
        .map(new JoinOps(_))
      val context =
        if (result.isSuccess) metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching, result.get.join)
        else metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching, join = name)
      // Throw exception after metrics. No join metadata is bound to be a critical failure.
      if (result.isFailure) {
        import ai.chronon.online.metrics
        context.withSuffix("join").increment(metrics.Metrics.Name.Exception)
        throw result.failed.get
      }
      context
        .withSuffix("join")
        .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { join =>
      import ai.chronon.online.metrics
      metrics.Metrics.Context(environment = "join.meta.fetch", join = join)
    }
  )

  def putJoinConf(join: Join): Unit = {
    val joinConfKeyForKvStore = join.keyNameForKvStore
    logger.info(s"uploading join conf to dataset: ${fetchContext.metadataDataset} by key:${joinConfKeyForKvStore}")
    fetchContext.kvStore.put(
      PutRequest(joinConfKeyForKvStore.getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 fetchContext.metadataDataset))
  }

  def listJoins(isOnline: Boolean = true): Future[Seq[String]] = {
    import ai.chronon.online.metrics

    val context = metrics.Metrics.Context(metrics.Metrics.Environment.MetaDataFetching)
    val startTimeMs = System.currentTimeMillis()

    def parseJoins(response: ListResponse): Seq[String] = {
      val result = response.values
        .map { seqListValues =>
          seqListValues
            .map(kv => new String(kv.valueBytes, StandardCharsets.UTF_8))
            .map(v => ThriftJsonCodec.fromJsonStr[Join](v, check = false, classOf[Join]))
            .filter(_.join.metaData.online == isOnline)
            .map(_.metaData.name)

        }
        .recover { case e: Exception =>
          import ai.chronon.online.metrics
          logger.error("Failed to list & parse joins from list response", e)
          context.withSuffix("join_list").increment(metrics.Metrics.Name.Exception)
          throw e
        }

      result.get
    }

    def doRetrieveAllListConfs(acc: mutable.ArrayBuffer[String],
                               paginationKey: Option[Any] = None): Future[Seq[String]] = {
      val propsMap = {
        paginationKey match {
          case Some(key) => Map(ListEntityType -> JoinKeyword, ContinuationKey -> key)
          case None      => Map(ListEntityType -> JoinKeyword)
        }
      }

      val listRequest = ListRequest(fetchContext.metadataDataset, propsMap)
      fetchContext.kvStore.list(listRequest).flatMap { response =>
        val joinSeq: Seq[String] = parseJoins(response)
        val newAcc = acc ++ joinSeq
        if (response.resultProps.contains(ContinuationKey)) {
          doRetrieveAllListConfs(newAcc, response.resultProps.get(ContinuationKey))
        } else {
          import ai.chronon.online.metrics
          context
            .withSuffix("join_list")
            .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Future.successful(newAcc)
        }
      }
    }

    doRetrieveAllListConfs(new mutable.ArrayBuffer[String]())
  }

  // key and value schemas
  def buildJoinCodecCache(onCreateFunc: Option[Try[JoinCodec] => Unit]): TTLCache[String, Try[JoinCodec]] = {

    val codecBuilder = { joinName: String =>
      getJoinConf(joinName)
        .map(_.join)
        .map(buildJoinCodec)
        .recoverWith { case th: Throwable =>
          Failure(
            new RuntimeException(
              s"Couldn't fetch joinName = ${joinName} or build join codec due to ${th.traceString}",
              th
            ))
        }
    }

    new TTLCache[String, Try[JoinCodec]](
      codecBuilder,
      { join: String =>
        import ai.chronon.online.metrics
        metrics.Metrics.Context(environment = "join.codec.fetch", join = join)
      },
      onCreateFunc = onCreateFunc
    )
  }

  def buildJoinCodec(joinConf: Join): JoinCodec = {
    val keyFields = new mutable.LinkedHashSet[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    // collect keyFields and valueFields from joinParts/GroupBys
    joinConf.joinPartOps.foreach { joinPart =>
      val servingInfoTry = getGroupByServingInfo(joinPart.groupBy.metaData.getName)
      servingInfoTry
        .map { servingInfo =>
          val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
          joinPart.leftToRight
            .mapValues(right => keySchema.fields.find(_.name == right).get.fieldType)
            .foreach { case (name, dType) =>
              val keyField = StructField(name, dType)
              keyFields.add(keyField)
            }
          val groupBySchemaBeforeDerivation: StructType =
            if (servingInfo.groupBy.aggregations == null) {
              servingInfo.selectedChrononSchema
            } else {
              servingInfo.outputChrononSchema
            }
          val baseValueSchema: StructType = if (!servingInfo.groupBy.hasDerivations) {
            groupBySchemaBeforeDerivation
          } else {
            val fields =
              buildDerivedFields(servingInfo.groupBy.derivationsScala, keySchema, groupBySchemaBeforeDerivation)
            StructType(s"groupby_derived_${servingInfo.groupBy.metaData.cleanName}", fields.toArray)
          }
          baseValueSchema.fields.foreach { sf =>
            valueFields.append(joinPart.constructJoinPartSchema(sf))
          }
        }
    }

    // gather key schema and value schema from external sources.
    Option(joinConf.join.onlineExternalParts).foreach { externals =>
      externals
        .iterator()
        .toScala
        .foreach { part =>
          val source = part.source

          def buildFields(schema: TDataType, prefix: String = ""): Seq[StructField] =
            DataType
              .fromTDataType(schema)
              .asInstanceOf[StructType]
              .fields
              .map(f => StructField(prefix + f.name, f.fieldType))

          buildFields(source.getKeySchema).foreach(f =>
            keyFields.add(f.copy(name = part.rightToLeft.getOrElse(f.name, f.name))))
          buildFields(source.getValueSchema, part.fullName + "_").foreach(f => valueFields.append(f))
        }
    }

    val joinName = joinConf.metaData.nameToFilePath
    val keySchema = StructType(s"${joinName.sanitize}_key", keyFields.toArray)
    val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
    val baseValueSchema = StructType(s"${joinName.sanitize}_value", valueFields.toArray)
    val baseValueCodec = serde.AvroCodec.of(AvroConversions.fromChrononSchema(baseValueSchema).toString)
    val joinCodec = JoinCodec(joinConf, keySchema, baseValueSchema, keyCodec, baseValueCodec)
    joinCodec
  }

  def getSchemaFromKVStore(dataset: String, key: String): serde.AvroCodec = {
    fetchContext.kvStore
      .getString(key, dataset, fetchContext.timeoutMillis)
      .recover { case e: java.util.NoSuchElementException =>
        logger.error(s"Failed to retrieve $key for $dataset. Is it possible that hasn't been uploaded?")
        throw e
      }
      .map(AvroCodec.of(_))
      .get
  }

  lazy val getStatsSchemaFromKVStore: TTLCache[(String, String), serde.AvroCodec] =
    new TTLCache[(String, String), serde.AvroCodec](
      { case (dataset, key) => getSchemaFromKVStore(dataset, key) },
      { _ => Metrics.Context(environment = "stats.serving_info.fetch") }
    )

  // pull and cache groupByServingInfo from the groupBy uploads
  lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
    new TTLCache[String, Try[GroupByServingInfoParsed]](
      { name =>
        val startTimeMs = System.currentTimeMillis()
        val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
        val metaData =
          fetchContext.kvStore
            .getString(Constants.GroupByServingInfoKey, batchDataset, fetchContext.timeoutMillis)
            .recover {
              case e: java.util.NoSuchElementException =>
                logger.error(
                  s"Failed to fetch metadata for $batchDataset, is it possible Group By Upload for $name has not succeeded?")
                throw e
              case e: Throwable =>
                logger.error(s"Failed to fetch metadata for $batchDataset", e)
                throw e
            }
        logger.info(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset")
        if (metaData.isFailure) {
          Failure(
            new RuntimeException(s"Couldn't fetch group by serving info for $batchDataset, " +
                                   "please make sure a batch upload was successful",
                                 metaData.failed.get))
        } else {
          import ai.chronon.online.metrics
          val groupByServingInfo = ThriftJsonCodec
            .fromJsonStr[GroupByServingInfo](metaData.get, check = true, classOf[GroupByServingInfo])
          metrics.Metrics
            .Context(metrics.Metrics.Environment.MetaDataFetching, groupByServingInfo.groupBy)
            .withSuffix("group_by")
            .distribution(metrics.Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Success(new GroupByServingInfoParsed(groupByServingInfo, partitionSpec))
        }
      },
      { gb =>
        import ai.chronon.online.metrics
        metrics.Metrics.Context(environment = "group_by.serving_info.fetch", groupBy = gb)
      }
    )

  def put(
      kVPairs: Map[String, Seq[String]],
      datasetName: String = MetadataDataset,
      batchSize: Int = CONF_BATCH_SIZE
  ): Future[Seq[Boolean]] = {
    val puts = kVPairs.map {
      case (k, v) => {
        logger.info(s"""Putting metadata for
             |dataset: $datasetName
             |key: $k
             |conf: $v""".stripMargin)
        val kBytes = k.getBytes()
        // The value is a single string by default, for NameByTeamEndPointName, it's a list of strings
        val vBytes = if (datasetName == NameByTeamEndPointName) {
          StringArrayConverter.stringsToBytes(v)
        } else {
          v.head.getBytes()
        }
        PutRequest(keyBytes = kBytes,
                   valueBytes = vBytes,
                   dataset = datasetName,
                   tsMillis = Some(System.currentTimeMillis()))
      }
    }.toSeq
    val putsBatches = puts.grouped(batchSize).toSeq
    logger.info(s"Putting ${puts.size} configs to KV Store, dataset=$datasetName")
    val futures = putsBatches.map(batch => fetchContext.kvStore.multiPut(batch))
    Future.sequence(futures).map(_.flatten)
  }

  def create(dataset: String): Unit = {
    try {
      logger.info(s"Creating dataset: $dataset")
      // TODO: this is actually just an async task. it doesn't block and thus we don't actually
      //  know if it successfully created the dataset
      fetchContext.kvStore.create(dataset)

      logger.info(s"Successfully created dataset: $dataset")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to create dataset: $dataset", e)
        throw e
    }
  }
}
