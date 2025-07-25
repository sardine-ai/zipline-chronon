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

package ai.chronon.spark.test.join

import ai.chronon.api.Extensions.{LabelPartsOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.spark.{Comparison, LabelJoin}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.test.TestUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{max, min}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class FeatureWithLabelJoinTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val spark: SparkSession = SparkSessionBuilder.build("FeatureWithLabelJoinTest", local = true)

  private val namespace = "final_join"
  private val tableName = "test_feature_label_join"
  private val tableUtils = TableUtils(spark)
  tableUtils.createDatabase(namespace)

  private val labelDS = "2022-10-30"
  private val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
  private val left = viewsGroupBy.groupByConf.sources.get(0)

  it should "final views" in {
    // create test feature join table
    val featureTable = s"${namespace}.${tableName}"
    createTestFeatureTable().write.saveAsTable(featureTable)

    val labelJoinConf = createTestLabelJoin(50, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelParts = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val labelDf = runner.computeLabelJoin()
    logger.info(" == First Run Label version 2022-10-30 == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn))
      .show()
    val featureDf = tableUtils.loadTable(joinConf.metaData.outputTable)
    logger.info(" == Features == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    val expectedFinal = featureDf.join(
      prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn)),
      labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn),
      "left_outer"
    )
    assertResult(computed, expectedFinal)

    // add another label version
    val secondRun = new LabelJoin(joinConf, tableUtils, "2022-11-11")
    val secondLabel = secondRun.computeLabelJoin()
    logger.info(" == Second Run Label version 2022-11-11 == ")
    secondLabel.show()
    val view = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView} order by label_ds")
    view.show()
    // listing 4 should not have any 2022-11-11 version labels
    assertEquals(null,
                 view
                   .where(view("label_ds") === "2022-11-11" && view("listing") === "4")
                   .select("label_listing_labels_dim_room_type")
                   .first()
                   .get(0))
    // 11-11 label record number should be same as 10-30 label version record number
    assertEquals(view.where(view("label_ds") === "2022-10-30").count(),
                 view.where(view("label_ds") === "2022-11-11").count())
    // listing 5 should not not have any label
    assertEquals(null,
                 view
                   .where(view("listing") === "5")
                   .select("label_ds")
                   .first()
                   .get(0))
  }

  it should "final views with agg label" in {
    // create test feature join table
    val tableName = "label_agg_table"
    val featureTable = s"${namespace}.${tableName}"
    val featureRows = List(
      Row(1L, 24L, "US", "2022-10-02", "2022-10-02 16:00:00"),
      Row(1L, 20L, "US", "2022-10-03", "2022-10-03 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 41L, "US", "2022-10-02", "2022-10-02 22:00:00"),
      Row(3L, 19L, "CA", "2022-10-03", "2022-10-03 08:00:00"),
      Row(4L, 2L, "MX", "2022-10-02", "2022-10-02 18:00:00")
    )
    createTestFeatureTable(tableName, featureRows).write.saveAsTable(featureTable)

    val rows = List(
      Row(1L, 20L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(2L, 30L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(3L, 10L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(1L, 20L, "2022-10-03 11:00:00", "2022-10-03"),
      Row(2L, 35L, "2022-10-03 11:00:00", "2022-10-03"),
      Row(3L, 15L, "2022-10-03 11:00:00", "2022-10-03")
    )
    val leftSource = TestUtils
      .createViewsGroupBy(namespace, spark, tableName = "listing_view_agg", customRows = rows)
      .groupByConf
      .sources
      .get(0)
    val labelJoinConf = createTestAggLabelJoin(5)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      leftSource,
      joinParts = Seq.empty,
      labelParts = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, "2022-10-06")
    val labelDf = runner.computeLabelJoin()
    logger.info(" == Label DF == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn))
      .show()
    val featureDf = tableUtils.loadTable(joinConf.metaData.outputTable)
    logger.info(" == Features DF == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    val expectedFinal = featureDf.join(
      prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn)),
      labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn),
      "left_outer"
    )
    assertResult(computed, expectedFinal)

    // add new labels
    val newLabelRows = List(
      Row(1L, 0, "2022-10-07", "2022-10-07 11:00:00"),
      Row(2L, 2, "2022-10-07", "2022-10-07 11:00:00"),
      Row(3L, 2, "2022-10-07", "2022-10-07 11:00:00")
    )
    TestUtils.createOrUpdateLabelGroupByWithAgg(namespace, spark, 5, "listing_labels_agg", newLabelRows)
    val runner2 = new LabelJoin(joinConf, tableUtils, "2022-10-07")
    val updatedLabelDf = runner2.computeLabelJoin()
    updatedLabelDf.show()
  }

  private def assertResult(computed: DataFrame, expected: DataFrame): Unit = {
    logger.info(" == Computed == ")
    computed.show()
    logger.info(" == Expected == ")
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds", "label_ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info("diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  private def prefixColumnName(df: DataFrame,
                               prefix: String = "label_",
                               exceptions: Array[String] = null): DataFrame = {
    logger.info("exceptions")
    logger.info(exceptions.mkString(", "))
    val renamedColumns = df.columns
      .map(col => {
        if (exceptions.contains(col) || col.startsWith(prefix)) {
          df(col)
        } else {
          df(col).as(s"$prefix$col")
        }
      })
    df.select(renamedColumns: _*)
  }

  def createTestLabelJoin(startOffset: Int,
                          endOffset: Int,
                          groupByTableName: String = "listing_labels"): ai.chronon.api.LabelParts = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }

  def createTestAggLabelJoin(windowSize: Int,
                             groupByTableName: String = "listing_labels_agg"): ai.chronon.api.LabelParts = {
    val labelGroupBy = TestUtils.createOrUpdateLabelGroupByWithAgg(namespace, spark, windowSize, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = windowSize,
      leftEndOffset = windowSize
    )
  }

  def createTestFeatureTable(tableName: String = tableName, customRows: List[Row] = List.empty): DataFrame = {
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
    val rows = if (customRows.isEmpty) {
      List(
        Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
        Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
        Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00"),
        Row(4L, 2L, "MX", "2022-10-02", "2022-10-02 18:00:00"),
        Row(5L, 139L, "EU", "2022-10-01", "2022-10-01 22:00:00"),
        Row(1L, 24L, "US", "2022-10-02", "2022-10-02 16:00:00")
      )
    } else customRows
    TestUtils.makeDf(spark, schema, rows)
  }
}
