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

package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online._
import ai.chronon.online.serde._
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.spark.GenericRowHandler
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.api.java.function.{MapPartitionsFunction, VoidFunction2}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import ai.chronon.online.metrics

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.{lang, util}
import java.util.Base64
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

// micro batching destroys and re-creates these objects repeatedly through ForeachBatchWriter and MapFunction
// this allows for re-use
object LocalIOCache {
  private var fetcher: Fetcher = null
  private var kvStore: KVStore = null
  def getOrSetFetcher(builderFunc: () => Fetcher): Fetcher = {
    if (fetcher == null) {
      fetcher = builderFunc()
    }
    fetcher
  }

  def getOrSetKvStore(builderFunc: () => KVStore): KVStore = {
    if (kvStore == null) {
      kvStore = builderFunc()
    }
    kvStore
  }
}

class JoinSourceRunner(groupByConf: api.GroupBy, conf: Map[String, String] = Map.empty, debug: Boolean, lagMillis: Int)(
    implicit
    session: SparkSession,
    apiImpl: Api)
    extends Serializable {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val context: metrics.Metrics.Context =
    metrics.Metrics.Context(metrics.Metrics.Environment.GroupByStreaming, groupByConf)

  private case class Schemas(leftStreamSchema: StructType,
                             leftSourceSchema: StructType,
                             joinSchema: StructType,
                             joinSourceSchema: StructType)
      extends Serializable

  val valueZSchema: api.StructType = groupByConf.dataModel match {
    case api.DataModel.EVENTS   => servingInfoProxy.valueChrononSchema
    case api.DataModel.ENTITIES => servingInfoProxy.mutationValueChrononSchema
  }
  val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
    case api.DataModel.ENTITIES => Constants.MutationFields.map(_.name) -> Constants.MutationTimeColumn
    case api.DataModel.EVENTS   => Seq.empty[String] -> Constants.TimeColumn
  }

  val keyColumns: Array[String] = groupByConf.keyColumns.toScala.toArray
  val valueColumns: Array[String] = groupByConf.aggregationInputs ++ additionalColumns

  private def getProp(prop: String, default: String) = session.conf.get(s"spark.chronon.stream.chain.${prop}", default)

  // when true, we will use the event time of the event to fetchJoin, otherwise we use the current time
  private val useEventTimeForQuery: Boolean = getProp("event_time_query", "true").toBoolean

  // this is the logical timestamp for a set of rows in a micro-batch
  // we will apply any delay based on this timestamp at this percentile
  private val timePercentile: Double = getProp("time_percentile", "0.95").toDouble

  // each micro-batch will be delayed by this amount of time
  // if the micro-batch is already delayed by more than this amount no delay will be added
  private val minimumQueryDelayMs: Int = getProp("query_delay_ms", "0").toInt

  // we will add this shift to the timestamp of the query before issuing fetchJoin
  // in theory this will cause online offline skew, but it is needed when timestamps of the events to join
  private val queryShiftMs: Int = getProp("query_shift_ms", "0").toInt

  // Micro batch interval - users can tune for lowering latency - or maximizing batch size
  private val microBatchIntervalMillis: Int = getProp("batch_interval_millis", "1000").toInt

  private case class PutRequestHelper(inputSchema: StructType) extends Serializable {
    @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    private val keyIndices: Array[Int] = keyColumns.map(inputSchema.fieldIndex)
    private val valueIndices: Array[Int] = valueColumns.map(inputSchema.fieldIndex)
    private val tsIndex: Int = inputSchema.fieldIndex(eventTimeColumn)
    private val keySparkSchema: StructType = StructType(keyIndices.map(inputSchema))
    private val keySchema: api.StructType = SparkConversions.toChrononStruct("key", keySparkSchema)

    @transient private lazy val keyToBytes: Any => Array[Byte] =
      AvroConversions.encodeBytes(keySchema, GenericRowHandler.func)
    @transient private lazy val valueToBytes: Any => Array[Byte] =
      AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    private val streamingDataset: String = groupByConf.streamingDataset

    def toPutRequest(input: Row): KVStore.PutRequest = {
      val keys = keyIndices.map(input.get)
      val values = valueIndices.map(input.get)

      context.distribution(metrics.Metrics.Name.PutKeyNullPercent, (keys.count(_ == null) * 100) / keys.length)
      context.distribution(metrics.Metrics.Name.PutValueNullPercent, (values.count(_ == null) * 100) / values.length)

      val ts = input.get(tsIndex).asInstanceOf[Long]
      val keyBytes = keyToBytes(keys)
      val valueBytes = valueToBytes(values)
      if (debug) {
        val gson = new Gson()
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
        val pstFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("America/Los_Angeles"))
        logger.info(s"""
             |dataset: $streamingDataset
             |keys: ${gson.toJson(keys)}
             |values: ${gson.toJson(values)}
             |keyBytes: ${Base64.getEncoder.encodeToString(keyBytes)}
             |valueBytes: ${Base64.getEncoder.encodeToString(valueBytes)}
             |ts: $ts|  UTC: ${formatter.format(Instant.ofEpochMilli(ts))}| PST: ${pstFormatter.format(
          Instant.ofEpochMilli(ts))}
             |""".stripMargin)
      }
      KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
    }
  }

  def outputSchema(inputSchema: StructType, query: api.Query)(implicit session: SparkSession): StructType = {
    if (query.selects == null) {
      inputSchema
    } else {
      val allSelects = query.selects.toScala
      val selects = allSelects.map { case (name, expr) => s"(${expr.toLowerCase}) AS $name" }.toSeq
      session.createDataFrame(session.sparkContext.emptyRDD[Row], inputSchema).selectExpr(selects: _*).schema
    }
  }

  private def enrichQuery(query: Query): Query = {
    val enrichedQuery = query.deepCopy()
    if (groupByConf.streamingSource.get.getJoinSource.getJoin.getLeft.isSetEntities) {
      enrichedQuery.selects.put(Constants.ReversalColumn, Constants.ReversalColumn)
      enrichedQuery.selects.put(Constants.MutationTimeColumn, Constants.MutationTimeColumn)
    } else if (query.isSetTimeColumn) {
      enrichedQuery.selects.put(Constants.TimeColumn, enrichedQuery.timeColumn)
    }
    enrichedQuery
  }

  private def buildSchemas(leftSchema: StructType): Schemas = {
    val source = groupByConf.streamingSource
    assert(source.get.isSetJoinSource, s"No JoinSource found in the groupBy: ${groupByConf.metaData.name}")
    assert(source.isDefined, s"No streaming source present in the groupBy: ${groupByConf.metaData.name}")

    val joinSource: JoinSource = source.get.getJoinSource
    val left: Source = joinSource.getJoin.getLeft
    assert(left.topic != null, "join source left side should have a topic")

    // for entities there is reversal and mutation column additionally
    val reversalField: StructField = StructField(Constants.ReversalColumn, BooleanType)
    val mutationTsField: StructField = StructField(Constants.MutationTimeColumn, LongType)
    val mutationFields: StructType = StructType(Seq(reversalField, mutationTsField))
    var leftStreamSchema: StructType = leftSchema
    if (left.isSetEntities) {
      leftStreamSchema = StructType((mutationFields ++ leftStreamSchema).distinct)
    }
    val leftSourceSchema: StructType = outputSchema(leftStreamSchema, enrichQuery(left.query)) // apply same thing

    // joinSchema = leftSourceSchema ++ joinCodec.valueSchema
    val joinCodec: JoinCodec =
      apiImpl
        .buildFetcher(debug)
        .metadataStore
        // immediately fails if the codec has partial error to avoid using stale codec
        .buildJoinCodec(joinSource.getJoin, refreshOnFail = false)
    val joinValueSchema: StructType = SparkConversions.fromChrononSchema(joinCodec.valueSchema)
    val joinSchema: StructType = StructType(leftSourceSchema ++ joinValueSchema)
    val joinSourceSchema: StructType = outputSchema(joinSchema, enrichQuery(joinSource.query))

    // GroupBy -> JoinSource (Join + outer_query)
    // Join ->
    //   Join.left -> (left.(table, mutation_stream, etc) + inner_query)
    logger.info(s"""
       |Schemas across chain of transformations
       |leftSchema:
       |  ${leftSchema.catalogString}
       |left stream Schema:
       |  ${leftStreamSchema.catalogString}
       |left schema after applying left query:
       |  ${leftSourceSchema.catalogString}
       |join schema:
       |  ${joinSchema.catalogString}
       |join schema after applying joinSource.query:
       |  ${joinSourceSchema.catalogString}
       |""".stripMargin)

    Schemas(leftStreamSchema, leftSourceSchema, joinSchema, joinSourceSchema)
  }

  private def servingInfoProxy: GroupByServingInfoParsed =
    apiImpl.buildFetcher(debug).metadataStore.getGroupByServingInfo(groupByConf.getMetaData.getName).get

  private def decode(dataStream: DataStream): DataStream = {
    val streamDecoder = apiImpl.streamDecoder(servingInfoProxy)

    val df = dataStream.df
    val ingressContext = context.withSuffix("ingress")
    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[Mutation]
    val deserialized: Dataset[Mutation] = df
      .as[Array[Byte]]
      .map { arr =>
        ingressContext.increment(metrics.Metrics.Name.RowCount)
        ingressContext.count(metrics.Metrics.Name.Bytes, arr.length)
        try {
          streamDecoder.fromBytes(arr)
        } catch {
          case ex: Throwable =>
            logger.info(s"Error while decoding streaming events from stream: ${dataStream.topicInfo.name}")
            ex.printStackTrace()
            ingressContext.incrementException(ex)
            null
        }
      }
      .filter { mutation =>
        lazy val bothNull = mutation.before != null && mutation.after != null
        lazy val bothSame = mutation.before sameElements mutation.after
        (mutation != null) && (!bothNull || !bothSame)
      }
    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    logger.info(s"""
         | streaming source: ${groupByConf.streamingSource.get}
         | streaming dataset: ${groupByConf.streamingDataset}
         | stream schema: ${streamSchema.catalogString}
         |""".stripMargin)

    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(Encoders.row(streamSchema))
    dataStream.copy(df = des)
  }

  private def internalStreamBuilder(streamType: String): StreamBuilder = {
    val suppliedBuilder = apiImpl.generateStreamBuilder(streamType)
    if (suppliedBuilder == null) {
      if (streamType == "kafka") {
        KafkaStreamBuilder
      } else {
        throw new RuntimeException(
          s"Couldn't access builder for type $streamType. Please implement one by overriding Api.generateStreamBuilder")
      }
    } else {
      suppliedBuilder
    }
  }

  private def buildStream(topic: TopicInfo): DataStream =
    internalStreamBuilder(topic.messageBus).from(topic)(session, conf)

  def percentile(arr: Array[Long], p: Double): Option[Long] = {
    if (arr == null || arr.length == 0) return None
    val sorted = arr.sorted
    val k = math.ceil((sorted.length - 1) * p).toInt
    Some(sorted(k))
  }

  def chainedStreamingQuery: DataStreamWriter[Row] = {
    val joinSource = groupByConf.streamingSource.get.getJoinSource
    val left = joinSource.join.left
    val topic = TopicInfo.parse(left.topic)

    val stream = buildStream(topic)
    val decoded = decode(stream)

    val leftStreamingQuery = groupByConf.buildLeftStreamingQuery(left.query, decoded.df.schema.fieldNames.toSeq)

    def applyQuery(df: DataFrame, query: api.Query): DataFrame = {
      val queryParts = groupByConf.buildQueryParts(query)
      logger.info(s"""
           |decoded schema: ${decoded.df.schema.catalogString}
           |queryParts: $queryParts
           |df schema: ${df.schema.prettyJson}
           |""".stripMargin)

      // apply left.query
      val selected = queryParts.selects.map(_.toSeq).map(exprs => df.selectExpr(exprs: _*)).getOrElse(df)
      selected.filter(queryParts.wheres.map("(" + _ + ")").mkString(" AND "))
    }

    val leftSource: Dataset[Row] = applyQuery(decoded.df, left.query)
    // key format joins/<team>/join_name
    val joinRequestName = joinSource.join.metaData.getName.replaceFirst("\\.", "/")
    logger.info(s"Upstream join request name: $joinRequestName")

    val tableUtils = TableUtils(session)
    // the decoded schema is in lower case
    val reqColumns = tableUtils.getColumnsFromQuery(leftStreamingQuery).map(_.toLowerCase).distinct

    val leftSchema = StructType(
      decoded.df.schema
        .filter(field =>
          reqColumns
            // handle nested struct, only the parent struct is needed here
            .map(col => if (col.contains(".")) col.split("\\.")(0) else col)
            .contains(field.name))
        .toSet
        .toArray
    )

    val schemas = buildSchemas(leftSchema)
    val joinChrononSchema = SparkConversions.toChrononSchema(schemas.joinSchema)
    val joinEncoder: Encoder[Row] = Encoders.row(schemas.joinSchema)
    val joinFields = schemas.joinSchema.fieldNames
    val leftColumns = schemas.leftSourceSchema.fieldNames
    logger.info(s"""
         |left columns ${leftColumns.mkString(",")}
         |reqColumns ${reqColumns.mkString(",")}
         |Fetching upstream join to enrich the stream... Fetching lag time: $lagMillis
         |""".stripMargin)

    // todo: add proper timestamp to the fetcher
    val leftTimeIndex = leftColumns.indexWhere(_ == eventTimeColumn)
    val enriched = leftSource.mapPartitions(
      new MapPartitionsFunction[Row, Row] {
        override def call(rows: util.Iterator[Row]): util.Iterator[Row] = {
          val shouldSample = Math.random() <= 0.1
          val fetcher = LocalIOCache.getOrSetFetcher { () =>
            logger.info(s"Initializing Fetcher. ${System.currentTimeMillis()}")
            context.increment("chain.fetcher.init")
            apiImpl.buildFetcher(debug = debug)
          }

          val rowsScala = rows.toScala.toArray
          val requests = rowsScala.map { row =>
            val keyMap = row.getValuesMap[AnyRef](leftColumns)
            val eventTs = row.get(leftTimeIndex).asInstanceOf[Long]
            context.distribution(metrics.Metrics.Name.LagMillis, System.currentTimeMillis() - eventTs)
            val ts = if (useEventTimeForQuery) Some(eventTs) else None
            Fetcher.Request(joinRequestName, keyMap, atMillis = ts.map(_ + queryShiftMs))
          }

          val microBatchTimestamp =
            percentile(rowsScala.map(_.get(leftTimeIndex).asInstanceOf[Long]), timePercentile)
          if (microBatchTimestamp.isDefined) {
            val microBatchLag = System.currentTimeMillis() - microBatchTimestamp.get
            context.distribution(metrics.Metrics.Name.BatchLagMillis, microBatchLag)

            if (minimumQueryDelayMs > 0 && microBatchLag >= 0 && microBatchLag < minimumQueryDelayMs) {
              val sleepMillis = minimumQueryDelayMs - microBatchLag
              Thread.sleep(sleepMillis)
              context.distribution(metrics.Metrics.Name.QueryDelaySleepMillis, sleepMillis)
            }
          }

          if (debug && shouldSample) {
            requests.foreach(request => logger.info(s"request: ${request.keys}, ts: ${request.atMillis}"))
          }

          val responsesFuture = fetcher.fetchJoin(requests = requests.toSeq)
          // this might be potentially slower, but spark doesn't work when the internal derivation functionality triggers
          // its own spark session, or when it passes around objects
          val responses = Await.result(responsesFuture, 5.second)

          if (debug && shouldSample) {
            logger.info(s"responses/request size: ${responses.size}/${requests.length}\n  responses: ${responses}")
            responses.foreach(response =>
              logger.info(
                s"request: ${response.request.keys}, ts: ${response.request.atMillis}, values: ${response.values}"))
          }
          responses.iterator.map { response =>
            val responseMap = response.values.get
            val allFields = response.request.keys ++ responseMap
            Fetcher.logResponseStats(response, context)

            SparkConversions
              .toSparkRow(joinFields.map(f => allFields.getOrElse(f, null)),
                          api.StructType.from("record", joinChrononSchema))
              .asInstanceOf[Row]
          }.toJava
        }
      },
      joinEncoder
    )

    val joinSourceDf = applyQuery(enriched, joinSource.query)
    val writer = joinSourceDf.writeStream.outputMode("append").trigger(Trigger.ProcessingTime(microBatchIntervalMillis))
    val putRequestHelper = PutRequestHelper(joinSourceDf.schema)

    def emitRequestMetric(request: PutRequest, context: metrics.Metrics.Context): Unit = {
      request.tsMillis.foreach { ts: Long =>
        context.distribution(metrics.Metrics.Name.FreshnessMillis, System.currentTimeMillis() - ts)
        context.increment(metrics.Metrics.Name.RowCount)
        context.distribution(metrics.Metrics.Name.ValueBytes, request.valueBytes.length)
        context.distribution(metrics.Metrics.Name.KeyBytes, request.keyBytes.length)
      }
    }

    writer.foreachBatch {
      new VoidFunction2[DataFrame, java.lang.Long] {
        override def call(df: DataFrame, l: lang.Long): Unit = {
          val kvStore = LocalIOCache.getOrSetKvStore { () => apiImpl.genKvStore }
          val data = df.collect()
          val putRequests = data.map(putRequestHelper.toPutRequest)
          if (debug) {
            logger.info(s" Final df size to write: ${data.length}")
            logger.info(s" Size of putRequests to kv store- ${putRequests.length}")
          } else {
            putRequests.foreach(request => emitRequestMetric(request, context.withSuffix("egress")))
            kvStore.multiPut(putRequests)
          }
        }
      }
    }
  }
}
