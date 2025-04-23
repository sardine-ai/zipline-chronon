package ai.chronon.spark.test.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch._
import ai.chronon.spark.test.{DataFrameGen, TableTestUtils}
import ai.chronon.spark.{GroupBy, Join, _}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

class LabelJoinV2Test extends AnyFlatSpec {

  import ai.chronon.spark.submission

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = submission.SparkSessionBuilder.build("LabelJoinV2Test", local = true)

  private val tableUtils = TableTestUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val thirtyOneDaysAgo = tableUtils.partitionSpec.minus(today, new Window(31, TimeUnit.DAYS))
  private val fortyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS))
  private val thirtyThreeDaysAgo = tableUtils.partitionSpec.minus(today, new Window(33, TimeUnit.DAYS))
  private val thirtySevenDaysAgo = tableUtils.partitionSpec.minus(today, new Window(37, TimeUnit.DAYS))
  private val fortyThreeDaysAgo = tableUtils.partitionSpec.minus(today, new Window(43, TimeUnit.DAYS))
  private val fortyFourDaysAgo = tableUtils.partitionSpec.minus(today, new Window(44, TimeUnit.DAYS))
  private val fortySevenDaysAgo = tableUtils.partitionSpec.minus(today, new Window(47, TimeUnit.DAYS))
  private val fiftyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(50, TimeUnit.DAYS))
  private val sixtyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  it should "test single label part and window" in {
    val namespace = "label_joinv2_single"
    tableUtils.createDatabase(namespace)

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val labelsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT,
      backfillStartDate = fiftyDaysAgo
    )

    val labelParts = Builders.LabelPart(
      labels = Seq(Builders.JoinPart(groupBy = labelsGroupBy))
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 2000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      labelParts = labelParts,
      metaData = Builders.MetaData(name = "test.item_snapshot_features", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    // Now compute the snapshots for the label join
    GroupBy.computeBackfill(labelsGroupBy, today, tableUtils)
    val labelGbOutputTable = labelsGroupBy.metaData.outputTable
    tableUtils.sql(s"SELECT * FROM $labelGbOutputTable").show()

    // Now compute the label join for thirty three days ago (label ds)
    val labelDateRange = new api.DateRange(thirtyThreeDaysAgo, thirtyThreeDaysAgo)
    val labelJoin = new LabelJoinV2(joinConf, tableUtils, labelDateRange)
    val labelComputed = labelJoin.compute()
    println("Label computed::")
    labelComputed.show()

    val joinOutputTable = joinConf.metaData.outputTable

    val expected =
      s"""
         | SELECT j.*, gb.time_spent_ms_sum_7d as label__unit_test_item_views_time_spent_ms_sum_7d FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortyDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$thirtyThreeDaysAgo") as gb
         | on j.item = gb.item
         |""".stripMargin

    val expectedDf = tableUtils.sql(expected)
    println("Expected::")
    expectedDf.show()

    val diff = Comparison.sideBySide(labelComputed, expectedDf, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      logger.info(s"Actual count: ${labelComputed.count()}")
      logger.info(s"Expected count: ${expectedDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  it should "test multiple label parts and windows" in {
    val namespace = "label_joinv2_multiple"
    tableUtils.createDatabase(namespace)

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_2"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_2", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val labelsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_test2", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT,
      backfillStartDate = fiftyDaysAgo
    )

    val labelsGroupBy2 = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MAX,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS), new Window(14, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_2_test2", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT,
      backfillStartDate = fiftyDaysAgo
    )

    val labelParts = Builders.LabelPart(
      labels = Seq(Builders.JoinPart(groupBy = labelsGroupBy), Builders.JoinPart(groupBy = labelsGroupBy2))
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 2000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      labelParts = labelParts,
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    // Now compute the snapshots for the label joins
    GroupBy.computeBackfill(labelsGroupBy, today, tableUtils)
    val labelGbOutputTable = labelsGroupBy.metaData.outputTable
    tableUtils.sql(s"SELECT * FROM $labelGbOutputTable").show()

    GroupBy.computeBackfill(labelsGroupBy2, today, tableUtils)
    val labelGbOutputTable2 = labelsGroupBy2.metaData.outputTable
    tableUtils.sql(s"SELECT * FROM $labelGbOutputTable2").show()

    // Now compute the label join for thirty three days ago (label ds)
    val labelDateRange = new api.DateRange(thirtyThreeDaysAgo, thirtyThreeDaysAgo)
    val labelJoin = new LabelJoinV2(joinConf, tableUtils, labelDateRange)
    val labelComputed = labelJoin.compute()
    println("Label computed::")
    labelComputed.show()

    val joinOutputTable = joinConf.metaData.outputTable

    // Expected output is different for each day
    // 7 days ago there is a label from both groupBys -- gb.time_spent_ms_sum_7d, gb2.time_spent_ms_max_7d
    // 10 days ago there is a label from one groupBy -- gb.time_spent_ms_sum_10d
    // 14 days ago there is a label from one groupBy -- gb2.time_spent_ms_max_14d
    val expected =
      s"""
         | SELECT
         |    j.*,
         |    gb.time_spent_ms_sum_7d as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    gb2.time_spent_ms_max_7d as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortyDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$thirtyThreeDaysAgo") as gb
         | on j.item = gb.item
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable2 WHERE ds = "$thirtyThreeDaysAgo") as gb2
         | on j.item = gb2.item
         |
         | UNION
         |
         | SELECT
         |    j.*,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    gb.time_spent_ms_sum_10d as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortyThreeDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$thirtyThreeDaysAgo") as gb
         | on j.item = gb.item
         |
         | UNION
         |
         | SELECT
         |    j.*,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    gb2.time_spent_ms_max_14d as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortySevenDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable2 WHERE ds = "$thirtyThreeDaysAgo") as gb2
         | on j.item = gb2.item
         |
         |""".stripMargin

    val expectedDf = tableUtils.sql(expected)
    println("Expected::")
    expectedDf.show()

    val diff = Comparison.sideBySide(labelComputed, expectedDf, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      logger.info(s"Actual count: ${labelComputed.count()}")
      logger.info(s"Expected count: ${expectedDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())

    // Now test that we correctly append the label column for the longer window without losing shorter windows
    // when the job "gets ahead". We have a label for 7d, but in 3 days after the initial job the 10d window
    // Should get appended (i.e. the 10d column goes from all null to having values without losing the 7d values)

    // compute the label join for thirty days ago (label ds)
    val labelDateRange2 = new api.DateRange(monthAgo, monthAgo)
    val labelJoin2 = new LabelJoinV2(joinConf, tableUtils, labelDateRange2)
    val labelComputed2 = labelJoin2.compute()
    println("Label computed (second run)::")
    labelComputed2.show()

    val expected2 =
      s"""
         | SELECT
         |    j.*,
         |    gb.time_spent_ms_sum_7d as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    gb2.time_spent_ms_max_7d as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$thirtySevenDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$monthAgo") as gb
         | on j.item = gb.item
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable2 WHERE ds = "$monthAgo") as gb2
         | on j.item = gb2.item
         |
         | UNION
         |
         | SELECT
         |    j.*,
         |    gb_old.time_spent_ms_sum_7d as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    gb.time_spent_ms_sum_10d as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    gb2_old.time_spent_ms_max_7d as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortyDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$monthAgo") as gb
         | on j.item = gb.item
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable WHERE ds = "$thirtyThreeDaysAgo") as gb_old
         | on j.item = gb_old.item
         |   LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable2 WHERE ds = "$thirtyThreeDaysAgo") as gb2_old
         | on j.item = gb2_old.item
         |
         | UNION
         |
         | SELECT
         |    j.*,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_7d,
         |    null as label__unit_test_item_views_test2_time_spent_ms_sum_10d,
         |    null as label__unit_test_item_views_2_test2_time_spent_ms_max_7d,
         |    gb2.time_spent_ms_max_14d as label__unit_test_item_views_2_test2_time_spent_ms_max_14d
         | FROM
         | (SELECT * FROM $joinOutputTable WHERE ds = "$fortyFourDaysAgo") as j
         | LEFT OUTER JOIN
         | (SELECT * FROM $labelGbOutputTable2 WHERE ds = "$monthAgo") as gb2
         | on j.item = gb2.item
         |
         |""".stripMargin

    val expectedDf2 = tableUtils.sql(expected2)
    println("Expected (second run)::")
    expectedDf2.show()

    val diff2 = Comparison.sideBySide(labelComputed2, expectedDf2, List("item", "ts", "ds"))

    if (diff2.count() > 0) {
      logger.info(s"Actual count: ${labelComputed2.count()}")
      logger.info(s"Expected count: ${expectedDf2.count()}")
      logger.info(s"Diff count: ${diff2.count()}")
      diff2.show()
    }

    assertEquals(0, diff2.count())
  }

  it should "test temporal label parts" in {
    val namespace = "label_joinv2_temporal"
    tableUtils.createDatabase(namespace)

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_temporal"
    DataFrameGen.events(spark, viewsSchema, count = 5000, partitions = 60).save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = sixtyDaysAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_temporal_features", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val labelsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(2, TimeUnit.HOURS), new Window(1, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_temporal_labels", namespace = namespace),
      accuracy = Accuracy.TEMPORAL,
      backfillStartDate = fiftyDaysAgo
    )

    logger.info(s"Labels group by: ${labelsGroupBy.accuracy} ${labelsGroupBy.inferredAccuracy}")

    val labelParts = Builders.LabelPart(
      labels = Seq(Builders.JoinPart(groupBy = labelsGroupBy))
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 2000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      labelParts = labelParts,
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    println("Join computed::")
    computed.show()

    val joinOutputTable = joinConf.metaData.outputTable

    val expectedJO =
      s"""SELECT * from $joinOutputTable where ds = '$thirtyOneDaysAgo'""".stripMargin

    val expectedDfX = tableUtils.sql(expectedJO)
    println("Expected JO SQL::")
    expectedDfX.show()

    // Now compute the label join for thirty three days ago (label ds)
    val labelDateRange = new api.DateRange(monthAgo, monthAgo)
    val labelJoin = new LabelJoinV2(joinConf, tableUtils, labelDateRange)
    val labelComputed = labelJoin.compute()
    println(s"Label computed for labelDs: ${monthAgo}")
    labelComputed.show()
    val oneDay = 24 * 60 * 60 * 1000
    val twoHours = 2 * 60 * 60 * 1000
    val fiveMinutes = 5 * 60 * 1000
    val oneHour = 60 * 60 * 1000

    val expected =
      s"""
         |WITH
         |   join_output AS (SELECT item, ts, ds, user_unit_test_item_views_temporal_features_time_spent_ms_average from $joinOutputTable where ds = '$thirtyOneDaysAgo')
         |
         |SELECT join_output.item,
         |        join_output.ts,
         |        join_output.ds,
         |        join_output.user_unit_test_item_views_temporal_features_time_spent_ms_average,
         |        SUM(
         |          CASE
         |            WHEN views.ts >= ((CAST(join_output.ts/$fiveMinutes AS LONG) * $fiveMinutes)) AND views.ts < (join_output.ts + 2 * 60 * 60 * 1000)
         |            THEN views.time_spent_ms
         |            ELSE NULL
         |          END
         |        ) as label__unit_test_item_views_temporal_labels_time_spent_ms_sum_2h,
         |        SUM(
         |          CASE
         |            WHEN views.ts >= ((CAST(join_output.ts/$oneHour AS LONG) * $oneHour)) AND views.ts < (join_output.ts + 24 * 60 * 60 * 1000)
         |            THEN views.time_spent_ms
         |            ELSE NULL
         |          END
         |        ) as label__unit_test_item_views_temporal_labels_time_spent_ms_sum_1d
         |     FROM join_output left outer join (SELECT * FROM $viewsTable WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds BETWEEN '$thirtyOneDaysAgo' and '$monthAgo') as views
         |     ON join_output.item = views.item
         |     GROUP BY join_output.item, join_output.ts, join_output.ds, join_output.user_unit_test_item_views_temporal_features_time_spent_ms_average
         |""".stripMargin

    val expectedDf = tableUtils.sql(expected)
    println("Expected::")
    expectedDf.show()

    labelComputed.cache()
    expectedDf.cache()

    val diff = Comparison.sideBySide(labelComputed, expectedDf, List("item", "ts", "ds"))
    diff.cache()
    val diffCount = diff.count()

    val joinOutputDf = tableUtils.sql(s"SELECT * FROM $joinOutputTable")
    val viewsDf = tableUtils.sql(s"SELECT * FROM $viewsTable")

    if (diffCount > 0) {
      logger.info(s"Actual count: ${labelComputed.count()}")
      logger.info(s"Expected count: ${expectedDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")

      val firstItem = diff.select(diff("item")).limit(1).collect()(0).getString(0)
      logger.info(s"First diff item: $firstItem")

      logger.info(s"First diff item in join output")
      joinOutputDf.filter(joinOutputDf("item") === firstItem).show()

      logger.info(s"First diff item in views")
      viewsDf.filter(viewsDf("item") === firstItem).show()

      diff.show()
    }

    assertEquals(0, diffCount)
  }

}
