package ai.chronon.spark.test.stats.drift

import ai.chronon
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.DriftMetric
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.PartitionSpec
import ai.chronon.api.Window
import ai.chronon.online.KVStore
import ai.chronon.online.stats.DriftStore
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.stats.drift.SummaryUploader
import ai.chronon.spark.stats.drift.scripts.PrepareData
import ai.chronon.spark.utils.InMemoryKvStore
import ai.chronon.spark.utils.MockApi
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.ScalaJavaConversions.IteratorOps

class DriftTest extends AnyFlatSpec with Matchers {

  val namespace = "drift_test"
  implicit val spark: SparkSession = SparkSessionBuilder.build(namespace, local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)
  tableUtils.createDatabase(namespace)

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

  "end_to_end" should "fetch prepare anomalous data, summarize, upload and fetch without failures" in {

    // generate anomalous data (join output)
    val prepareData = PrepareData(namespace)
    val join = prepareData.generateAnomalousFraudJoin
    val df = prepareData.generateFraudSampleData(100000, "2023-01-01", "2023-01-30", join.metaData.loggedTable)
    df.show(10, truncate = false)

    // compute summary table and packed table (for uploading)
    Summarizer.compute(join.metaData, ds = "2023-01-30", useLogs = true)
    val summaryTable = join.metaData.summaryTable
    val packedTable = join.metaData.packedSummaryTable
    showTable(summaryTable)
    showTable(packedTable)

    // mock api impl for online fetching and uploading
    val kvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = InMemoryKvStore.build("drift_test", () => null)
      result
    }
    val api = new MockApi(kvStoreFunc, namespace)

    // create necessary tables in kvstore
    val kvStore = api.genKvStore
    kvStore.create(Constants.MetadataDataset)
    kvStore.create(Constants.TiledSummaryDataset)

    // upload join conf
    api.buildFetcher().putJoinConf(join)

    // upload summaries
    val uploader = new SummaryUploader(tableUtils.loadTable(packedTable),api)
    uploader.run()

    // test drift store methods
    val driftStore = new DriftStore(api.genKvStore)

    // fetch keys
    val tileKeys = driftStore.tileKeysForJoin(join)
    println(tileKeys)

    // fetch summaries
    val startMs = PartitionSpec.daily.epochMillis("2023-01-01")
    val endMs = PartitionSpec.daily.epochMillis("2023-01-29")
    val summariesFuture = driftStore.getSummaries(join, Some(startMs), Some(endMs), None)
    val summaries = Await.result(summariesFuture, Duration.create(10, TimeUnit.SECONDS))
    println(summaries)

    // fetch drift series
    val driftSeriesFuture = driftStore.getDriftSeries(
      join.metaData.nameToFilePath,
      DriftMetric.JENSEN_SHANNON,
      lookBack = new Window(7, chronon.api.TimeUnit.DAYS),
      startMs,
      endMs
    )
    val driftSeries = Await.result(driftSeriesFuture.get, Duration.create(10, TimeUnit.SECONDS))

    val (nulls, totals) = driftSeries.iterator.foldLeft(0 -> 0) {
      case ((nulls, total), s) =>
        val currentNulls = s.getPercentileDriftSeries.iterator().toScala.count(_ == null)
        val currentCount = s.getPercentileDriftSeries.size()
        (nulls + currentNulls, total + currentCount)
    }

    println(
      s"""drift totals: $totals
         |drift nulls: $nulls
         |""".stripMargin.red)

    println("Drift series fetched successfully".green)

    totals should be > 0
    nulls.toDouble / totals.toDouble should be < 0.6

    val summarySeriesFuture = driftStore.getSummarySeries(
      join.metaData.nameToFilePath,
      startMs,
      endMs
    )
    val summarySeries = Await.result(summarySeriesFuture.get, Duration.create(10, TimeUnit.SECONDS))
    val (summaryNulls, summaryTotals) = summarySeries.iterator.foldLeft(0 -> 0) {
      case ((nulls, total), s) =>
        if (s.getPercentiles == null) {
          (nulls + 1) -> (total + 1)
        } else {
          val currentNulls = s.getPercentiles.iterator().toScala.count(_ == null)
          val currentCount = s.getPercentiles.size()
          (nulls + currentNulls, total + currentCount)
        }
    }
    println(
      s"""summary ptile totals: $summaryTotals
         |summary ptile nulls: $summaryNulls
         |""".stripMargin)

    summaryTotals should be > 0
    summaryNulls.toDouble / summaryTotals.toDouble should be < 0.1
    println("Summary series fetched successfully".green)
  }
}