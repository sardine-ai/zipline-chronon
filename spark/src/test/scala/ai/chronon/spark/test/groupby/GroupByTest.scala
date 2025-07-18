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

package ai.chronon.spark.test.groupby

import ai.chronon.aggregator.test.{CStream, Column, NaiveAggregator}
import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.api.Aggregation
import ai.chronon.api.Builders
import ai.chronon.api.Constants
import ai.chronon.api.DoubleType
import ai.chronon.api.Extensions._
import ai.chronon.api.IntType
import ai.chronon.api.LongType
import ai.chronon.api.Operation
import ai.chronon.api.PartitionSpec
import ai.chronon.api.Source
import ai.chronon.api.StringType
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.api.PartitionRange
import ai.chronon.online.serde.RowWrapper
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.test.{DataFrameGen, TestUtils}
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.spark.catalog.TableUtils

import scala.collection.mutable

class GroupByTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  lazy val spark: SparkSession = submission.SparkSessionBuilder.build("GroupByTest", local = true)
  val tableUtils: TableUtils = TableUtils(spark)
  implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  it should "snapshot entities" in {
    val schema = List(
      Column("user", StringType, 10),
      Column(Constants.TimeColumn, LongType, 100), // ts = last 100 days
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen.entities(spark, schema, 10000, 10) // ds = last 10 days
    val viewName = "test_group_by_entities"
    df.createOrReplaceTempView(viewName)
    val aggregations: Seq[Aggregation] = Seq(
      Builders
        .Aggregation(Operation.AVERAGE, "session_length", Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)))

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    val actualDf = groupBy.snapshotEntities
    val expectedDf = df.sqlContext.sql(s"""
                                          |SELECT user,
                                          |       ds,
                                          |       AVG(IF(ts  >= (unix_timestamp(ds, 'yyyy-MM-dd') - (86400*(10 - 1))) * 1000, session_length, null)) AS session_length_average_10d,
                                          |       AVG(session_length) as session_length_average
                                          |FROM $viewName
                                          |WHERE ts < unix_timestamp(ds, 'yyyy-MM-dd') * 1000 + 86400 * 1000
                                          |GROUP BY user, ds
                                          |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  it should "snapshot events" in {
    val schema = List(
      Column("user", StringType, 10), // ts = last 10 days
      Column("session_length", IntType, 2),
      Column("rating", DoubleType, 2000)
    )

    val outputDates = CStream.genPartitions(10, tableUtils.partitionSpec)

    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 100)
    df.drop("ts") // snapshots don't need ts.
    val viewName = "test_group_by_snapshot_events"
    df.createOrReplaceTempView(viewName)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT,
                           "session_length",
                           Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)),
      Builders.Aggregation(Operation.UNIQUE_COUNT,
                           "session_length",
                           Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)),
      Builders.Aggregation(Operation.SUM, "session_length", Seq(new Window(10, TimeUnit.DAYS)))
    )

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    val actualDf = groupBy.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))
    val outputDatesRdd: RDD[Row] = spark.sparkContext.parallelize(outputDates.map(Row(_)))
    val outputDatesDf = spark.createDataFrame(outputDatesRdd, StructType(Seq(StructField("ds", SparkStringType))))
    val datesViewName = "test_group_by_snapshot_events_output_range"
    outputDatesDf.createOrReplaceTempView(datesViewName)
    val expectedDf = df.sqlContext.sql(s"""
                                          |select user,
                                          |       $datesViewName.ds,
                                          |       COUNT(DISTINCT session_length) as session_length_approx_unique_count,
                                          |       COUNT(DISTINCT session_length) as session_length_unique_count,
                                          |       COUNT(DISTINCT IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) as session_length_approx_unique_count_10d,
                                          |       COUNT(DISTINCT IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) as session_length_unique_count_10d,
                                          |       SUM(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) AS session_length_sum_10d,
                                          |       SUM(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, rating, null)) AS rating_sum_10d
                                          |FROM $viewName CROSS JOIN $datesViewName
                                          |WHERE ts < unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') * 1000 + ${tableUtils.partitionSpec.spanMillis}
                                          |group by user, $datesViewName.ds
                                          |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  it should "events last k" in {
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("listing_view", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 10000, partitions = 180)
    eventDf.createOrReplaceTempView("events_last_k")

    val querySchema = List(Column("user", StringType, 10))
    val queryDf = DataFrameGen.events(spark, querySchema, count = 1000, partitions = 180)
    queryDf.createOrReplaceTempView("queries_last_k")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "listing_view", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.COUNT, "listing_view", Seq(WindowUtils.Unbounded))
    )
    val keys = Seq("user").toArray
    val groupBy =
      new GroupBy(aggregations,
                  keys,
                  eventDf.selectExpr("user", "ts", "concat(ts, \" \", listing_view) as listing_view"))
    val resultDf = groupBy.temporalEvents(queryDf)
    val computed = resultDf.select("user", "ts", "listing_view_last30", "listing_view_count")
    computed.show()

    val expected = eventDf.sqlContext.sql("""
         |SELECT
         |      events_last_k.user as user,
         |      queries_last_k.ts as ts,
         |      COLLECT_LIST(concat(CAST(events_last_k.ts AS STRING), " ", events_last_k.listing_view)) as listing_view_last30,
         |      SUM(case when events_last_k.listing_view <=> NULL then 0 else 1 end) as listing_view_count
         |FROM events_last_k CROSS JOIN queries_last_k
         |ON events_last_k.user = queries_last_k.user
         |WHERE events_last_k.ts < queries_last_k.ts
         |GROUP BY events_last_k.user, queries_last_k.ts
         |""".stripMargin)

    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("user", "ts"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println("diff result rows last_k_test")
      diff.show()
      diff.rdd.foreach { row =>
        val gson = new Gson()
        val computed =
          Option(row(4)).map(_.asInstanceOf[mutable.WrappedArray[String]].toArray).getOrElse(Array.empty[String])
        val expected =
          Option(row(5))
            .map(_.asInstanceOf[mutable.WrappedArray[String]].toArray.sorted.reverse.take(30))
            .getOrElse(Array.empty[String])
        val computedCount = Option(row(2)).map(_.asInstanceOf[Long]).getOrElse(0)
        val expectedCount = Option(row(3)).map(_.asInstanceOf[Long]).getOrElse(0)
        val computedStr = gson.toJson(computed)
        val expectedStr = gson.toJson(expected)
        if (computedStr != expectedStr) {
          println(s"""
                     |computed [$computedCount]: ${gson.toJson(computed)}
                     |expected [$expectedCount]: ${gson.toJson(expected)}
                     |""".stripMargin)
        }
        assertEquals(gson.toJson(computed), gson.toJson(expected))
      }
    }
  }
  it should "temporal events" in {
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )

    val eventDf = DataFrameGen.events(spark, eventSchema, count = 10000, partitions = 180)

    val querySchema = List(Column("user", StringType, 10))

    val queryDf = DataFrameGen.events(spark, querySchema, count = 1000, partitions = 180)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "session_length",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))))

    val keys = Seq("user").toArray
    val groupBy = new GroupBy(aggregations, keys, eventDf)
    val resultDf = groupBy.temporalEvents(queryDf)

    val keyBuilder = FastHashing.generateKeyBuilder(keys, eventDf.schema)
    // naive aggregation for equivalence testing
    // this will basically explode on even moderate large table
    val queriesByKey: RDD[(KeyWithHash, Array[Long])] = queryDf
      .where("user IS NOT NULL")
      .rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map {
          _.getAs[Long](Constants.TimeColumn)
        }.toArray
      }

    val eventsByKey: RDD[(KeyWithHash, Iterator[RowWrapper])] = eventDf.rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map(SparkConversions.toChrononRow(_, groupBy.tsIndex)).toIterator
      }

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator =
      new NaiveAggregator(groupBy.windowAggregator, windows, tailHops)
    val naiveRdd = queriesByKey.leftOuterJoin(eventsByKey).flatMap {
      case (key, (queries: Array[Long], events: Option[Iterator[RowWrapper]])) =>
        val irs = naiveAggregator.aggregate(events.map(_.toSeq).orNull, queries)
        queries.zip(irs).map { case (query: Long, ir: Array[Any]) =>
          (key.data :+ query, groupBy.windowAggregator.finalize(ir))
        }
    }
    val naiveDf = groupBy.toDf(naiveRdd, Seq((Constants.TimeColumn, SparkLongType)))

    val diff = Comparison.sideBySide(naiveDf, resultDf, List("user", Constants.TimeColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  // Test that the output of Group by with Step Days is the same as the output without Steps (full data range)
  it should "step days consistency" in {
    val (source, endPartition) = createTestSource()

    val tableUtils = TableUtils(spark)
    val testSteps = Option(30)
    val namespace = "test_steps"
    val diff = Comparison.sideBySide(
      tableUtils.sql(
        s"SELECT * FROM ${backfill(name = "unit_test_item_views_steps", source = source, endPartition = endPartition, namespace = namespace, tableUtils = tableUtils, stepDays = testSteps)}"),
      tableUtils.sql(
        s"SELECT * FROM ${backfill(name = "unit_test_item_views_no_steps", source = source, endPartition = endPartition, namespace = namespace, tableUtils = tableUtils)}"),
      List("item", tableUtils.partitionColumn)
    )
    if (diff.count() != 0) {
      diff.show(100)
    }
    assertEquals(0, diff.count())
  }

  it should "group by analyzer" in {
    val (source, endPartition) = createTestSource(30)

    val tableUtils = TableUtils(spark)
    val namespace = "test_analyzer"
    val groupByConf = getSampleGroupBy("unit_analyze_test_item_views", source, namespace)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val (aggregationsMetadata, _) =
      new Analyzer(tableUtils, groupByConf, endPartition, today).analyzeGroupBy(groupByConf)
    val outputTable = backfill(name = "unit_analyze_test_item_views",
                               source = source,
                               endPartition = endPartition,
                               namespace = namespace,
                               tableUtils = tableUtils)
    val df = tableUtils.sql(s"SELECT * FROM  $outputTable")
    val expectedSchema = df.schema.fields.map(field => s"${field.name} => ${field.dataType}")
    aggregationsMetadata
      .map(agg => s"${agg.name} => ${agg.columnType}")
      .foreach(s => assertTrue(expectedSchema.contains(s)))

    // feature name is constructed by input_column_operation_window
    // assert feature columns attributes mapping
    aggregationsMetadata.foreach(aggregation => {
      assertTrue(aggregation.name.contains(aggregation.operation.toLowerCase))
      assertTrue(aggregation.name.contains(aggregation.inputColumn.toLowerCase))
    })
  }

  it should "group by no aggregation analyzer" in {
    val (source, endPartition) = createTestSource(30)
    val testName = "unit_analyze_test_item_no_agg"

    val tableUtils = TableUtils(spark)
    val namespace = "test_analyzer"
    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = null,
      metaData = Builders.MetaData(name = testName, namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(60, TimeUnit.DAYS))
    )
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val (aggregationsMetadata, _) =
      new Analyzer(tableUtils, groupByConf, endPartition, today).analyzeGroupBy(groupByConf)

    print(aggregationsMetadata.mkString("Array(", ", ", ")"))
    assertTrue(aggregationsMetadata.length == 2)

    val columns = aggregationsMetadata.map(a => a.name -> a.columnType).toMap
    assertEquals(Map(
                   "time_spent_ms" -> LongType,
                   "price" -> DoubleType
                 ),
                 columns)
  }

  // test that OrderByLimit and OrderByLimitTimed serialization works well with Spark's data type
  it should "first k last k top k bottom k approx unique count" in {
    val (source, endPartition) = createTestSource()

    val tableUtils = TableUtils(spark)
    val namespace = "test_order_by_limit"
    val aggs = Seq(
      Builders.Aggregation(operation = Operation.LAST_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.FIRST_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.TOP_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.BOTTOM_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ))
    )
    backfill(name = "unit_test_serialization",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)
  }

  private def createTestSource(windowSize: Int = 365, suffix: String = ""): (Source, String) = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val startPartition = tableUtils.partitionSpec.minus(today, new Window(windowSize, TimeUnit.DAYS))
    val endPartition = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val sourceSchema = List(
      Column("user", StringType, 10000),
      Column("item", StringType, 100),
      Column("time_spent_ms", LongType, 5000),
      Column("price", DoubleType, 100)
    )
    val namespace = "chronon_test"
    val sourceTable = s"$namespace.test_group_by_steps$suffix"

    tableUtils.createDatabase(namespace)
    DataFrameGen.events(spark, sourceSchema, count = 1000, partitions = 200).save(sourceTable)
    val source = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts", "item", "time_spent_ms", "price"),
                             startPartition = startPartition),
      table = sourceTable
    )
    (source, endPartition)
  }

  def backfill(name: String,
               source: Source,
               namespace: String,
               endPartition: String,
               tableUtils: TableUtils,
               stepDays: Option[Int] = None,
               additionalAgg: Seq[Aggregation] = Seq.empty): String = {
    tableUtils.createDatabase(namespace)
    val groupBy = getSampleGroupBy(name, source, namespace, additionalAgg)

    GroupBy.computeBackfill(
      groupBy,
      endPartition = endPartition,
      tableUtils = tableUtils,
      stepDays = stepDays
    )
    s"$namespace.$name"
  }

  def getSampleGroupBy(name: String,
                       source: Source,
                       namespace: String,
                       additionalAgg: Seq[Aggregation] = Seq.empty): ai.chronon.api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN,
                             inputColumn = "ts",
                             windows = Seq(
                               new Window(15, TimeUnit.DAYS),
                               new Window(60, TimeUnit.DAYS),
                               WindowUtils.Unbounded
                             )),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ) ++ additionalAgg,
      metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(60, TimeUnit.DAYS))
    )
  }

  // Test percentile Impl on Spark.
  it should "percentiles" in {
    val (source, endPartition) = createTestSource(suffix = "_percentile")
    val tableUtils = TableUtils(spark)
    val namespace = "test_percentiles"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.APPROX_PERCENTILE,
        inputColumn = "ts",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "128", "percentiles" -> "[0.5, 0.25, 0.75]")
      )
    )
    backfill(name = "unit_test_group_by_percentiles",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)
  }

  it should "approx histograms" in {
    val (source, endPartition) = createTestSource(suffix = "_approx_histogram")
    val tableUtils = TableUtils(spark)
    val namespace = "test_approx_histograms"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.APPROX_FREQUENT_K,
        inputColumn = "item",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_FREQUENT_K,
        inputColumn = "ts",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_FREQUENT_K,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      )
    )
    backfill(name = "unit_test_group_by_approx_histograms",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)

    val histogramValues = spark
      .sql("""
          |select explode(map_values(item_approx_frequent_k_15d)) as item_values
          |from test_approx_histograms.unit_test_group_by_approx_histograms
          |""".stripMargin)
      .map(row => row.getAs[Long]("item_values"))(Encoders.scalaLong)
      .collect()
      .toSet

    assert(histogramValues.nonEmpty)
    assert(!histogramValues.contains(0))
  }

  it should "replace join source" in {
    val namespace = "replace_join_source_ns"
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

    val joinSource = TestUtils.getParentJoin(spark, namespace, "parent_join_table", "parent_gb")
    val query = Builders.Query(startPartition = today)
    val chainingGroupBy = TestUtils.getTestGBWithJoinSource(joinSource, query, namespace, "chaining_gb")
    val newGroupBy =
      GroupBy.replaceJoinSource(chainingGroupBy, PartitionRange(today, today), tableUtils, computeDependency = false)

    assertEquals(joinSource.metaData.outputTable, newGroupBy.sources.get(0).table)
    assertEquals(joinSource.left.topic + Constants.TopicInvalidSuffix, newGroupBy.sources.get(0).topic)
    assertEquals(query, newGroupBy.sources.get(0).query)
  }

  it should "group by from chaining gb" in {
    val namespace = "test_chaining_gb"
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val joinName = "parent_join_table"
    val parentGBName = "parent_gb"

    val joinSource = TestUtils.getParentJoin(spark, namespace, joinName, parentGBName)
    val query = Builders.Query(startPartition = today)
    val chainingGroupBy = TestUtils.getTestGBWithJoinSource(joinSource, query, namespace, "user_viewed_price_gb")
    val newGroupBy = GroupBy.from(chainingGroupBy, PartitionRange(today, today), tableUtils, computeDependency = true)

    // verify parent join output table is computed and
    assertTrue(spark.catalog.tableExists(s"$namespace.parent_join_table"))
    val expectedSQL =
      s"""
         |WITH latestB AS (
         |    SELECT
         |        COALESCE(A.listing, '--null--') listing,
         |        A.user,
         |        MAX(A.ts) as ts,
         |        A.ds
         |    FROM
         |        $namespace.parent_join_table  A
         |    LEFT OUTER JOIN
         |       $namespace.views_table B ON A.listing = B.listing
         |    WHERE
         |        B.ts <= A.ts AND A.ds = '$today'
         |    GROUP BY
         |        A.listing, A.user, A.ds
         |)
         |SELECT
         |    IF(latestB.listing == '--null--', null, latestB.listing) as listing,
         |    latestB.user,
         |    latestB.ts,
         |    latestB.ds,
         |    C.parent_gb_price_last
         |FROM
         |    latestB
         |JOIN
         |   $namespace.parent_join_table C
         |ON
         |    latestB.listing = COALESCE(C.listing, '--null--') AND latestB.ts = C.ts
         |""".stripMargin
    val expectedInputDf = spark.sql(expectedSQL)
    println("Expected input DF: ")
    expectedInputDf.show()
    println("Computed input DF: ")
    newGroupBy.inputDf.show()

    val diff = Comparison.sideBySide(newGroupBy.inputDf, expectedInputDf, List("listing", "user", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${newGroupBy.inputDf.count()}")
      println(s"Expected count: ${expectedInputDf.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  it should "descriptive stats" in {
    val (source, endPartition) = createTestSource(suffix = "_descriptive_stats")
    val tableUtils = TableUtils(spark)
    val namespace = "test_descriptive_stats"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.VARIANCE,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      ),
      Builders.Aggregation(
        operation = Operation.SKEW,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      ),
      Builders.Aggregation(
        operation = Operation.KURTOSIS,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      )
    )
    backfill(name = "unit_test_group_by_descriptive_stats",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)
  }
}
