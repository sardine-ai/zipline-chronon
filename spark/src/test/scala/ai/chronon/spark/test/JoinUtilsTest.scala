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

package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Builders
import ai.chronon.api.Constants
import ai.chronon.api.PartitionSpec
import ai.chronon.online.PartitionRange
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.JoinUtils.contains_any
import ai.chronon.spark.JoinUtils.set_add
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import scala.util.Try

class JoinUtilsTest extends AnyFlatSpec {

  lazy val spark: SparkSession = SparkSessionBuilder.build("JoinUtilsTest", local = true)
  private val tableUtils = TableUtils(spark)
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
  private val namespace = "joinUtil"
  it should "udf set add" in {
    val data = Seq(
      Row(Seq("a", "b", "c"), "a"),
      Row(Seq("a", "b", "c"), "d"),
      Row(Seq("a", "b", "c"), null),
      Row(null, "a"),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("set", ArrayType(StringType))
      .add("item", StringType)
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
    val df: DataFrame = spark.createDataFrame(rdd, schema)

    val actual = df
      .select(set_add(col("set"), col("item")).as("new_set"))
      .collect()
      .map(_.getAs[mutable.WrappedArray[String]](0))

    val expected = Array(
      Seq("a", "b", "c"),
      Seq("a", "b", "c", "d"),
      Seq("a", "b", "c"),
      Seq("a"),
      null
    )

    expected.zip(actual).map { case (e, a) =>
      e == a
    }
  }

  it should "udf contains any" in {
    val data = Seq(
      Row(Seq("a", "b", "c"), Seq("a")),
      Row(Seq("a", "b", "c"), Seq("a", "b")),
      Row(Seq("a", "b", "c"), Seq("d")),
      Row(Seq("a", "b", "c"), null),
      Row(null, Seq("a")),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("array", ArrayType(StringType))
      .add("query", ArrayType(StringType))
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
    val df: DataFrame = spark.createDataFrame(rdd, schema)

    val actual = df
      .select(contains_any(col("array"), col("query")).as("result"))
      .collect()
      .map(_.getAs[Any](0))

    val expected = Array(
      true, true, false, null, false, null
    )

    expected.zip(actual).map { case (e, a) =>
      e == a
    }
  }

  private def testJoinScenario(leftSchema: StructType,
                               rightSchema: StructType,
                               keys: Seq[String],
                               isFailure: Boolean): Try[DataFrame] = {
    val df = Try(
      JoinUtils.coalescedJoin(
        // using empty dataframe is sufficient to test spark query planning
        spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), leftSchema),
        spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), rightSchema),
        keys
      ))
    if (isFailure) {
      assertTrue(df.isFailure)
    } else {
      assertTrue(df.isSuccess)
    }
    df
  }

  it should "coalesced join mismatched key columns" in {
    // mismatch data type on join keys
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", StringType)
        .add("col2", LongType),
      Seq("key"),
      isFailure = true
    )
  }

  it should "coalesced join mismatched shared columns" in {
    // mismatch data type on shared columns
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", StringType),
      Seq("key"),
      isFailure = true
    )
  }

  it should "coalesced join missing keys" in {
    // missing some keys
    testJoinScenario(
      new StructType()
        .add("key1", LongType)
        .add("key2", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key1", LongType)
        .add("col2", LongType),
      Seq("key1", "key2"),
      isFailure = true
    )
  }

  it should "coalesced join no shared columns" in {
    // test no shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col2", StringType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "coalesced join shared columns" in {
    // test shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", StringType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col3", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(4, df.get.columns.length)
  }

  it should "coalesced join one sided left" in {
    // test when left side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "coalesced join one sided right" in {
    // test when right side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      new StructType()
        .add("key", LongType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  it should "create join view" in {
    val finalViewName = "testCreateView"
    val leftTableName = "joinUtil.testFeatureTable"
    val rightTableName = "joinUtil.testLabelTable"
    tableUtils.createDatabase(namespace)
    TestUtils.createSampleFeatureTableDf(spark).write.saveAsTable(leftTableName)
    TestUtils.createSampleLabelTableDf(spark).write.saveAsTable(rightTableName)
    val keys = Array("listing_id", tableUtils.partitionColumn)

    JoinUtils.createOrReplaceView(finalViewName,
                                  leftTableName,
                                  rightTableName,
                                  keys,
                                  tableUtils,
                                  viewProperties = Map("featureTable" -> leftTableName, "labelTable" -> rightTableName))

    val view = tableUtils.sql(s"select * from $finalViewName")
    view.show()
    assertEquals(6, view.count())
    assertEquals(null,
                 view
                   .where(view("ds") === "2022-10-01" && view("listing_id") === "5")
                   .select("label_room_type")
                   .first()
                   .get(0))
    assertEquals("SUPER_HOST",
                 view
                   .where(view("ds") === "2022-10-07" && view("listing_id") === "1")
                   .select("label_host_type")
                   .first()
                   .get(0))

    val properties = tableUtils.getTableProperties(finalViewName)
    assertTrue(properties.isDefined)
    assertEquals(properties.get.get("featureTable"), Some(leftTableName))
    assertEquals(properties.get.get("labelTable"), Some(rightTableName))
  }

  it should "create latest label view" in {
    val finalViewName = "joinUtil.testFinalView"
    val leftTableName = "joinUtil.testFeatureTable2"
    val rightTableName = "joinUtil.testLabelTable2"
    tableUtils.createDatabase(namespace)
    TestUtils.createSampleFeatureTableDf(spark).write.saveAsTable(leftTableName)
    tableUtils.insertPartitions(TestUtils.createSampleLabelTableDf(spark),
                                rightTableName,
                                partitionColumns = Seq(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
    val keys = Array("listing_id", tableUtils.partitionColumn)

    JoinUtils.createOrReplaceView(
      finalViewName,
      leftTableName,
      rightTableName,
      keys,
      tableUtils,
      viewProperties = Map(Constants.LabelViewPropertyFeatureTable -> leftTableName,
                           Constants.LabelViewPropertyKeyLabelTable -> rightTableName)
    )
    val view = tableUtils.sql(s"select * from $finalViewName")
    view.show()
    assertEquals(6, view.count())

    //verity latest label view
    val latestLabelView = "testLatestLabel"
    JoinUtils.createLatestLabelView(latestLabelView,
                                    finalViewName,
                                    tableUtils,
                                    propertiesOverride = Map("newProperties" -> "value"))
    val latest = tableUtils.sql(s"select * from $latestLabelView")
    latest.show()
    assertEquals(2, latest.count())
    assertEquals(0, latest.filter(latest("listing_id") === "3").count())
    assertEquals("2022-11-22", latest.where(latest("ds") === "2022-10-07").select("label_ds").first().get(0))
    // label_ds should be unique per ds + listing
    val removeDup = latest.dropDuplicates(Seq("label_ds", "ds"))
    assertEquals(removeDup.count(), latest.count())

    val properties = tableUtils.getTableProperties(latestLabelView)
    assertTrue(properties.isDefined)
    assertEquals(properties.get.get(Constants.LabelViewPropertyFeatureTable), Some(leftTableName))
    assertEquals(properties.get.get("newProperties"), Some("value"))
  }

  it should "filter columns" in {
    val testDf = createSampleTable()
    val filter = Array("listing", "ds", "feature_review")
    val filteredDf = JoinUtils.filterColumns(testDf, filter)
    assertTrue(filteredDf.schema.fieldNames.sorted sameElements filter.sorted)
  }

  it should "get ranges to fill" in {
    tableUtils.createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = "joinUtil.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val startPartition = "2023-04-15"
    val endPartition = "2023-08-01"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = startPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangesToFill(leftSource, tableUtils, endPartition)
    assertEquals(range, PartitionRange(startPartition, endPartition))
  }

  it should "get ranges to fill with override" in {
    tableUtils.createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = "joinUtil.queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 50)
      .save(itemQueriesTable)

    val startPartition = "2023-04-15"
    val startPartitionOverride = "2023-08-01"
    val endPartition = "2023-08-08"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = startPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangesToFill(leftSource, tableUtils, endPartition, Some(startPartitionOverride))
    assertEquals(range, PartitionRange(startPartitionOverride, endPartition))
  }

  import ai.chronon.api.{LongType, StringType, StructField, StructType}

  def createSampleTable(tableName: String = "testSampleTable"): DataFrame = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing", LongType),
        StructField("feature_review", LongType),
        StructField("feature_locale", StringType),
        StructField("ds", StringType),
        StructField("ts", StringType)
      )
    )
    val rows = List(
      Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00")
    )
    TestUtils.makeDf(spark, schema, rows)
  }
}
