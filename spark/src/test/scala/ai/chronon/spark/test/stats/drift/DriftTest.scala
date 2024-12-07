package ai.chronon.spark.test.stats.drift

import ai.chronon
import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.DriftMetric
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.PartitionSpec
import ai.chronon.api.TileSummarySeries
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
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.ScalaJavaConversions.IteratorOps
import scala.util.ScalaJavaConversions.ListOps
import scala.util.ScalaJavaConversions.MapOps
import scala.util.Success

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
    val df = prepareData.generateFraudSampleData(600000, "2023-01-01", "2023-02-30", join.metaData.loggedTable)
    df.show(10, truncate = false)

    // mock api impl for online fetching and uploading
    val kvStoreFunc: () => KVStore = () => {
      // cannot reuse the variable - or serialization error
      val result = InMemoryKvStore.build("drift_test", () => null)
      result
    }
    val api = new MockApi(kvStoreFunc, namespace)

    // compute summary table and packed table (for uploading)
    Summarizer.compute(api, join.metaData, ds = "2023-02-30", useLogs = true)
    val summaryTable = join.metaData.summaryTable
    val packedTable = join.metaData.packedSummaryTable
    showTable(summaryTable)
    showTable(packedTable)

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
    val endMs = PartitionSpec.daily.epochMillis("2023-02-29")
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
    val summarySeries = Await.result(summarySeriesFuture.get, Duration.create(100, TimeUnit.SECONDS))
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

    val startTs=1673308800000L
    val endTs=1674172800000L
    val joinName = "risk.user_transactions.txn_join"
    val name = "dim_user_account_type"
    val window = new Window(10, ai.chronon.api.TimeUnit.HOURS)

    val joinPath = joinName.replaceFirst("\\.", "/")

    implicit val execContext = scala.concurrent.ExecutionContext.global
    val metric = ValuesMetric
    val maybeCurrentSummarySeries = driftStore.getSummarySeries(joinPath, startTs, endTs, Some(name))
    val maybeBaselineSummarySeries =
      driftStore.getSummarySeries(joinPath, startTs - window.millis, endTs - window.millis, Some(name))
    val result = (maybeCurrentSummarySeries, maybeBaselineSummarySeries) match {
      case (Success(currentSummarySeriesFuture), Success(baselineSummarySeriesFuture)) =>
        Future.sequence(Seq(currentSummarySeriesFuture, baselineSummarySeriesFuture)).map { merged =>
          val currentSummarySeries = merged.head
          val baselineSummarySeries = merged.last
          val isCurrentNumeric = currentSummarySeries.headOption.forall(checkIfNumeric)
          val isBaselineNumeric = baselineSummarySeries.headOption.forall(checkIfNumeric)

           val currentFeatureTs = {
            if (currentSummarySeries.isEmpty) Seq.empty
            else convertTileSummarySeriesToTimeSeries(currentSummarySeries.head, isCurrentNumeric, metric)
          }
          val baselineFeatureTs = {
            if (baselineSummarySeries.isEmpty) Seq.empty
            else convertTileSummarySeriesToTimeSeries(baselineSummarySeries.head, isBaselineNumeric, metric)
          }

          ComparedFeatureTimeSeries(name, isCurrentNumeric, baselineFeatureTs, currentFeatureTs)
        }
    }
    println(Await.result(result, Duration.create(10, TimeUnit.SECONDS)))
  }

  // this is clunky copy of code, but was necessary to run the logic end-to-end without mocking drift store
  // TODO move this into TimeSeriesControllerSpec and refactor that test to be more end-to-end.
  case class ComparedFeatureTimeSeries(feature: String,
                                       isNumeric: Boolean,
                                       baseline: Seq[TimeSeriesPoint],
                                       current: Seq[TimeSeriesPoint])

  sealed trait Metric

  /** Roll up over null counts */
  case object NullMetric extends Metric

  /** Roll up over raw values */
  case object ValuesMetric extends Metric


  case class TimeSeriesPoint(value: Double, ts: Long, label: Option[String] = None, nullValue: Option[Int] = None)

  def checkIfNumeric(summarySeries: TileSummarySeries): Boolean = {
    val ptiles = summarySeries.percentiles.toScala
    ptiles != null && ptiles.exists(_ != null)
  }



  private def convertTileSummarySeriesToTimeSeries(summarySeries: TileSummarySeries,
                                                   isNumeric: Boolean,
                                                   metric: Metric): Seq[TimeSeriesPoint] = {
    if (metric == NullMetric) {
      summarySeries.nullCount.toScala.zip(summarySeries.timestamps.toScala).map {
        case (nullCount, ts) => TimeSeriesPoint(0, ts, nullValue = Some(nullCount.intValue()))
      }
    } else {
      if (isNumeric) {
        val percentileSeriesPerBreak = summarySeries.percentiles.toScala
        val timeStamps = summarySeries.timestamps.toScala
        val breaks = DriftStore.breaks(20)
        percentileSeriesPerBreak.zip(breaks).flatMap{ case (percentileSeries, break) =>
            percentileSeries.toScala.zip(timeStamps).map{case (value, ts) => TimeSeriesPoint(value, ts, Some(break))}
        }
      } else {
        val histogramOfSeries = summarySeries.histogram.toScala
        val timeStamps = summarySeries.timestamps.toScala
        histogramOfSeries.flatMap{ case (label, values) =>
          values.toScala.zip(timeStamps).map{case (value, ts) => TimeSeriesPoint(value.toDouble, ts, Some(label))}
        }.toSeq
      }
    }
  }
}