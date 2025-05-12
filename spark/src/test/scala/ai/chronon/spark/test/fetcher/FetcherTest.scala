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

package ai.chronon.spark.test.fetcher

import ai.chronon.api
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.online.{fetcher, _}
import ai.chronon.online.fetcher.FetchContext
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.test.{OnlineUtils, SchemaEvolutionUtils}
import ai.chronon.spark.utils.MockApi
import ai.chronon.spark.{Join => _, _}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import java.util.concurrent.Executors
import java.{lang, util}
import scala.collection.Seq
import scala.concurrent.ExecutionContext

class FetcherTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val sessionName = "FetcherTest"
  val spark: SparkSession = submission.SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  // Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
  def compareTemporalFetch(joinConf: api.Join,
                           endDs: String,
                           namespace: String,
                           consistencyCheck: Boolean,
                           dropDsOnWrite: Boolean,
                           enableTiling: Boolean = false): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()

    val tilingEnabledFlagStore = new FlagStore {
      override def isSet(flagName: String, attributes: util.Map[String, String]): lang.Boolean = {
        if (flagName == FlagStoreConstants.TILING_ENABLED) {
          enableTiling
        } else {
          false
        }
      }
    }

    val mockApi = new MockApi(kvStoreFunc, namespace)
    mockApi.setFlagStore(tilingEnabledFlagStore)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")

    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        endDs,
                        jp.groupBy,
                        dropDsOnWrite = dropDsOnWrite,
                        tilingEnabled = enableTiling))

    // Extract queries for the EndDs from the computedJoin results and eliminating computed aggregation values
    val endDsEvents = {
      tableUtils.sql(
        s"SELECT * FROM $joinTable WHERE ts >= unix_timestamp('$endDs', '${tableUtils.partitionSpec.format}')")
    }
    val endDsQueries = endDsEvents.drop(endDsEvents.schema.fieldNames.filter(_.contains("unit_test")): _*)
    val keys = joinConf.leftKeyCols
    val keyIndices = keys.map(endDsQueries.schema.fieldIndex)
    val tsIndex = endDsQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new fetcher.MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    def buildRequests(lagMs: Int = 0): Array[Request] =
      endDsQueries.rdd
        .map { row =>
          val keyMap = keyIndices.indices.map { idx =>
            keys(idx) -> row.get(keyIndices(idx)).asInstanceOf[AnyRef]
          }.toMap
          val ts = row.get(tsIndex).asInstanceOf[Long]
          Request(joinConf.metaData.name, keyMap, Some(ts - lagMs))
        }
        .collect()

    val requests = buildRequests()

    if (consistencyCheck) {
      val lagMs = -100000
      val laggedRequests = buildRequests(lagMs)
      val laggedResponseDf =
        FetcherTestUtil.joinResponses(spark, laggedRequests, mockApi, samplePercent = 5, logToHive = true)._2
      val correctedLaggedResponse = laggedResponseDf
        .withColumn("ts_lagged", laggedResponseDf.col("ts_millis") + lagMs)
        .withColumn("ts_millis", col("ts_lagged"))
        .drop("ts_lagged")
      logger.info("corrected lagged response")
      correctedLaggedResponse.show()
      correctedLaggedResponse.save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))

      // build flattened log table
      SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, today)
      val flattenerJob = new LogFlattenerJob(spark, joinConf, today, mockApi.logTable, mockApi.schemaTable)
      flattenerJob.buildLogTable()

      // build consistency metrics
      val consistencyJob = new ConsistencyJob(spark, joinConf, today)
      val metrics = consistencyJob.buildConsistencyMetrics()
      logger.info(s"ooc metrics: $metrics".stripMargin)
      OnlineUtils.serveConsistency(tableUtils, inMemoryKvStore, today, joinConf)
    }
    // benchmark
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 10, useJavaFetcher = true)
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 10)

    // comparison
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      FetcherTestUtil.joinResponses(spark, requests, mockApi, useJavaFetcher = true, debug = true)._1.map { res =>
        val all: Map[String, AnyRef] =
          res.request.keys ++
            res.values.get ++
            Map(tableUtils.partitionColumn -> today) ++
            Map(Constants.TimeColumn -> lang.Long.valueOf(res.request.atMillis.get))
        val values: Array[Any] = columns.map(all.get(_).orNull)
        SparkConversions
          .toSparkRow(values, StructType.from("record", SparkConversions.toChrononSchema(endDsExpected.schema)))
          .asInstanceOf[GenericRow]
      }

    logger.info(endDsExpected.schema.pretty)

    val keyishColumns = keys.toList ++ List(tableUtils.partitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows.toSeq)
    var responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, endDsExpected.schema)
    if (endDs != today) {
      responseDf = responseDf.drop("ds").withColumn("ds", lit(endDs))
    }
    logger.info("expected:")
    endDsExpected.show()
    logger.info("response:")
    responseDf.show()

    val diff = Comparison.sideBySide(responseDf, endDsExpected, keyishColumns, aName = "online", bName = "offline")
    assertEquals(endDsQueries.count(), responseDf.count())
    if (diff.count() > 0) {
      logger.info("queries:")
      endDsQueries.show()
      logger.info(s"Total count: ${responseDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows:")
      diff
        .withTimeBasedColumn("ts_string", "ts", "yy-MM-dd HH:mm")
        .select("ts_string", diff.schema.fieldNames: _*)
        .show()
    }
    assertEquals(0, diff.count())
  }

  it should "test temporal fetch join deterministic" in {
    val namespace = "deterministic_fetch"
    val joinConf = FetcherTestUtil.generateMutationData(namespace, tableUtils, spark)
    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false, dropDsOnWrite = true)
  }

  it should "test temporal fetch join generated" in {
    val namespace = "generated_fetch"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday)
    compareTemporalFetch(joinConf,
                         tableUtils.partitionSpec.at(System.currentTimeMillis()),
                         namespace,
                         consistencyCheck = true,
                         dropDsOnWrite = false)
  }

  it should "test temporal tiled fetch join deterministic" in {
    val namespace = "deterministic_tiled_fetch"
    val joinConf = FetcherTestUtil.generateEventOnlyData(namespace, tableUtils, spark)
    compareTemporalFetch(joinConf,
                         "2021-04-10",
                         namespace,
                         consistencyCheck = false,
                         dropDsOnWrite = true,
                         enableTiling = true)
  }
}
