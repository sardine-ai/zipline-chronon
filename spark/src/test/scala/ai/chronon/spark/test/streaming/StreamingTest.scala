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

package ai.chronon.spark.test.streaming

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api._
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.test.{DataFrameGen, OnlineUtils}
import ai.chronon.spark.utils.InMemoryKvStore
import ai.chronon.spark.{Join => _, _}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.util.TimeZone
import scala.collection.JavaConverters.asScalaBufferConverter

object StreamingTest {
  def buildInMemoryKvStore(): InMemoryKvStore = {
    InMemoryKvStore.build("StreamingTest",
                          { () =>
                            import ai.chronon.spark.submission
                            TableUtils(submission.SparkSessionBuilder.build("StreamingTest", local = true))
                          })
  }
}

class StreamingTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  val spark: SparkSession = submission.SparkSessionBuilder.build("StreamingTest", local = true)
  val tableUtils: TableUtils = TableUtils(spark)
  val namespace = "streaming_test"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  tableUtils.partitionSpec.before(today)
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  it should "struct in streaming" in {
    tableUtils.createDatabase(namespace)
    val topicName = "fake_topic"
    val inMemoryKvStore = StreamingTest.buildInMemoryKvStore()
    val nameSuffix = "_struct_streaming_test"
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_$nameSuffix"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)

    itemQueriesDf.save(s"${itemQueriesTable}_tmp")
    val structLeftDf = tableUtils.sql(
      s"SELECT item, NAMED_STRUCT('item_repeat', item) as item_struct, ts, ds FROM ${itemQueriesTable}_tmp")
    structLeftDf.save(itemQueriesTable)
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$nameSuffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      topic = topicName,
      query = Builders.Query(
        selects = Seq(
          "str_arr" -> "transform(array(1, 2, 3), x -> CAST(x as STRING))",
          "time_spent_ms" -> "time_spent_ms",
          "item_struct" -> "NAMED_STRUCT('item_repeat', item)",
          "item" -> "item"
        ).toMap,
        startPartition = yearAgo
      )
    )
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    df.save(viewsTable)
    val gb = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "1"), inputColumn = "item_struct"),
        Builders.Aggregation(operation = Operation.HISTOGRAM, argMap = Map("k" -> "2"), inputColumn = "str_arr")
      ),
      metaData =
        Builders.MetaData(name = s"unit_test.item_views_$nameSuffix", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = gb, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features$nameSuffix", namespace = namespace, team = "item_team")
    )
    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)
    joinConf.joinParts.asScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, StreamingTest.buildInMemoryKvStore, namespace, today, jp.groupBy))
  }
}
