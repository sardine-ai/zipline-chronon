package ai.chronon.spark.test.stats.drift

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.spark.Extensions._
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.test.DataFrameGen
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DriftTest extends AnyFlatSpec with Matchers {

  private val namespace = "drift_test"
  "TestDataFrameGenerator" should "create a DataFrame with various column types including nulls" in {
    implicit val spark: SparkSession = SparkSessionBuilder.build("DriftTest", local = true)
    implicit val tableUtils: TableUtils = TableUtils(spark)
    tableUtils.createDatabase(namespace)

    try {
      val df = generateDataFrame(1000000, 10)

      // Check if DataFrame is created
      df.isEmpty shouldBe false
      df.show(10, truncate = false)

      val summarizer = new Summarizer(df)
      val result = summarizer.computeSummaryDf
      result.show()
    } finally {
      spark.stop()
    }
  }


  def generateDataFrame(numRows: Int, partitions: Int)(implicit spark: SparkSession): DataFrame = {

    val dollarTransactions = List(
      Column("user", api.StringType, 100),
      Column("user_name", api.StringType, 100),
      Column("amount_dollars", api.LongType, 100000),
      Column("item_prices", api.ListType(api.LongType), 1000),
      Column("category_item_prices", api.MapType(api.StringType, api.IntType), 100),
    )

    val txnsTable = s"$namespace.txns"
    spark.sql(s"DROP TABLE IF EXISTS $txnsTable")

    DataFrameGen.events(spark, dollarTransactions, numRows, partitions = partitions).save(txnsTable)
    TableUtils(spark).loadTable(txnsTable)
  }
}