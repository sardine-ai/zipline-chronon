package ai.chronon.spark.test.stats.drift

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.online.KVStore
import ai.chronon.spark.Extensions._
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.stats.drift.SummaryPacker
import ai.chronon.spark.stats.drift.SummaryUploader
import ai.chronon.spark.test.DataFrameGen
import ai.chronon.spark.test.MockApi
import ai.chronon.spark.test.MockKVStore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DriftTest extends AnyFlatSpec with Matchers {

  val namespace = "drift_test"
  implicit val spark: SparkSession = SparkSessionBuilder.build(namespace, local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)
  tableUtils.createDatabase(namespace)

  "TestDataFrameGenerator" should "create a DataFrame with various column types including nulls" in {
    try {
      val df = generateDataFrame(100000, 10, namespace)

      // Check if DataFrame is created
      df.isEmpty shouldBe false
      df.show(10, truncate = false)

      val summarizer = new Summarizer("drift_test_basic")
      val (result, summaryExprs) = summarizer.computeSummaryDf(df)
      result.show()
      val packer = new SummaryPacker("drift_test_basic", summaryExprs, summarizer.tileSize, summarizer.sliceColumns)
      val (packed, _) = packer.packSummaryDf(result)
      packed.show()

      val props = Map("is-time-sorted" -> "true")

      val kvStore: () => KVStore = () => {
        val result = new MockKVStore()
        result.create(Constants.DriftStatsTable, props)
        result
      }
      val api = new MockApi(kvStore, "drift_test_basic")

      val uploader = new SummaryUploader(packed,api)
      uploader.run()
    }
  }

  def showTable(name: String)(implicit tableUtils: TableUtils): Unit = {
    println(s"Showing table $name".yellow)
    val df = tableUtils.loadTable(name)
    val maxColNameLength = df.schema.fieldNames.map(_.length).max
    def pad(s: String): String = s.padTo(maxColNameLength, ' ')
    df.schema.fields.foreach { f =>
      println(s"    ${pad(f.name)} : ${f.dataType.typeName}".yellow)
    }

    df.show(10, truncate = false)
  }

  "PrepareData" should "should create fraud dataframe with anomalies without exceptions" in {

    val prepareData = PrepareData(namespace)
    val join = prepareData.generateAnomalousFraudJoin
    val df = prepareData.generateFraudSampleData(1000000, "2023-01-01", "2023-03-31", join.metaData.loggedTable)

    df.show(10, truncate = false)
    Summarizer.compute(join.metaData, ds = "2023-03-31", useLogs = true)

    val summaryTable = join.metaData.summaryTable
    val packedTable = join.metaData.packedSummaryTable

    showTable(summaryTable)
    showTable(packedTable)
  }

  // step1 - generate some data with prepare join
  // step2 - inject anomalies into the input data

  def generateDataFrame(numRows: Int, partitions: Int, namespace: String)(implicit spark: SparkSession): DataFrame = {

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