package controllers

import ai.chronon.api.TileDriftSeries
import ai.chronon.api.TileSeriesKey
import ai.chronon.api.TileSummarySeries
import ai.chronon.online.stats.DriftStore
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import org.scalatest.EitherValues
import org.scalatestplus.play._
import play.api.http.Status.BAD_REQUEST
import play.api.http.Status.OK
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._

import java.lang.{Double => JDouble}
import java.lang.{Long => JLong}
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class TimeSeriesControllerSpec extends PlaySpec with Results with EitherValues {

  // Create a stub ControllerComponents
  val stubCC: ControllerComponents = stubControllerComponents()

  implicit val ec: ExecutionContext = ExecutionContext.global

  // Create a mocked drift store
  val mockedStore: DriftStore = mock(classOf[DriftStore])
  val controller = new TimeSeriesController(stubCC, mockedStore)
  val mockCategories: Seq[String] = Seq("a", "b", "c")

  "TimeSeriesController's join ts lookup" should {

    "send 400 on an invalid metric choice" in {
      val invalid1 = controller.fetchJoin("my_join", 123L, 456L, "meow", "null", None, None).apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 = controller.fetchJoin("my_join", 123L, 456L, "skew", "null", None, None).apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid metric rollup" in {
      val invalid = controller.fetchJoin("my_join", 123L, 456L, "drift", "woof", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid time offset for drift metric" in {
      val invalid1 =
        controller.fetchJoin("my_join", 123L, 456L, "drift", "null", Some("Xh"), Some("psi")).apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 =
        controller.fetchJoin("my_join", 123L, 456L, "drift", "null", Some("-1h"), Some("psi")).apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid algorithm for drift metric" in {
      val invalid1 =
        controller.fetchJoin("my_join", 123L, 456L, "drift", "null", Some("10h"), Some("meow")).apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send 5xx on failed drift store lookup request" in {
      when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any())).thenReturn(Failure(new IllegalArgumentException("Some internal error")))

      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller.fetchJoin("my_join", startTs, endTs, "drift", "null", Some("10h"), Some("psi")).apply(FakeRequest())

      status(result) mustBe INTERNAL_SERVER_ERROR
    }

    "send valid results on a correctly formed model ts drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC

      val mockedDriftStoreResponse = generateDriftSeries(startTs, endTs, "my_join", 2, 3)
      when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any())).thenReturn(mockedDriftStoreResponse)

      val result =
        controller.fetchJoin("my_join", startTs, endTs, "drift", "value", Some("10h"), Some("psi")).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val modelTSResponse: Either[Error, JoinTimeSeriesResponse] = decode[JoinTimeSeriesResponse](bodyText)
      modelTSResponse.isRight mustBe true
      val response = modelTSResponse.right.value
      response.name mustBe "my_join"
      response.items.nonEmpty mustBe true

      val expectedLength = expectedHours(startTs, endTs)
      response.items.foreach { grpByTs =>
        grpByTs.items.isEmpty mustBe false
        grpByTs.items.foreach(featureTs => featureTs.points.length mustBe expectedLength)
      }
    }
  }

  "TimeSeriesController's feature ts lookup" should {

    "send 400 on an invalid metric choice" in {
      val invalid1 = controller.fetchFeature("my_join", "my_feature", 123L, 456L, "meow", "null", "raw", None, None).apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 = controller.fetchFeature("my_join", "my_feature", 123L, 456L, "skew", "null", "raw", None, None).apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid metric rollup" in {
      val invalid =
        controller.fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "woof", "raw", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid granularity" in {
      val invalid =
        controller.fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "null", "woof", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid time offset for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "null", "aggregates", Some("Xh"), Some("psi"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 =
        controller
          .fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "null", "aggregates", Some("-1h"), Some("psi"))
          .apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid algorithm for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "null", "aggregates", Some("10h"), Some("meow"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send 400 on an invalid granularity for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_join", "my_feature", 123L, 456L, "drift", "null", "raw", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send valid results on a correctly formed feature ts aggregate drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC

      val mockedDriftStoreResponse = generateDriftSeries(startTs, endTs, "my_join", 1, 1)
      when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any())).thenReturn(mockedDriftStoreResponse)

      val result =
        controller
          .fetchFeature("my_join", "my_feature_0", startTs, endTs, "drift", "null", "aggregates", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, FeatureTimeSeries] = decode[FeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature_0"
      val expectedLength = expectedHours(startTs, endTs)
      response.points.length mustBe expectedLength
    }

    "send valid results on a correctly formed numeric feature ts percentile drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC

      val mockedSummarySeriesResponseA = generateSummarySeries(startTs, endTs, "my_join", "my_groupby", "my_feature", ValuesMetric, true)
      val offset = Duration.apply(7, TimeUnit.DAYS)
      val mockedSummarySeriesResponseB =
        generateSummarySeries(startTs - offset.toMillis, endTs - offset.toMillis, "my_join", "my_groupby", "my_feature", ValuesMetric, true)
      when(mockedStore.getSummarySeries(any(), any(), any(), any())).thenReturn(mockedSummarySeriesResponseA, mockedSummarySeriesResponseB)

      val result =
        controller
          .fetchFeature("my_join", "my_feature", startTs, endTs, "drift", "value", "percentile", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature"
      response.current.length mustBe response.baseline.length
      response.current.zip(response.baseline).foreach {
        case (current, baseline) =>
          (current.ts - baseline.ts) mustBe offset.toMillis
      }

      // expect one entry per percentile for each time series point
      val expectedLength = DriftStore.breaks(20).length * expectedHours(startTs, endTs)
      response.current.length mustBe expectedLength
      response.baseline.length mustBe expectedLength
    }

    "send valid results on a correctly formed categorical feature ts percentile drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC

      val mockedSummarySeriesResponseA = generateSummarySeries(startTs, endTs, "my_join", "my_groupby", "my_feature", ValuesMetric, false)
      val offset = Duration.apply(7, TimeUnit.DAYS)
      val mockedSummarySeriesResponseB =
        generateSummarySeries(startTs - offset.toMillis, endTs - offset.toMillis, "my_join", "my_groupby", "my_feature", ValuesMetric, false)
      when(mockedStore.getSummarySeries(any(), any(), any(), any())).thenReturn(mockedSummarySeriesResponseA, mockedSummarySeriesResponseB)

      val result =
        controller
          .fetchFeature("my_join", "my_feature", startTs, endTs, "drift", "value", "percentile", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature"
      response.current.length mustBe response.baseline.length
      // expect one entry per category for each time series point
      val expectedLength = mockCategories.length * expectedHours(startTs, endTs)
      response.current.length mustBe expectedLength
      response.current.zip(response.baseline).foreach {
        case (current, baseline) =>
          (current.ts - baseline.ts) mustBe offset.toMillis
          current.label.isEmpty mustBe false
          baseline.label.isEmpty mustBe false
      }
    }
  }

  private def expectedHours(startTs: Long, endTs: Long): Long = {
    Duration(endTs - startTs, TimeUnit.MILLISECONDS).toHours
  }

  private def generateDriftSeries(startTs: Long, endTs: Long, join: String, numGroups: Int, numFeaturesPerGroup: Int): Try[Future[Seq[TileDriftSeries]]] = {
    val result = for {
      group <- 0 until numGroups
      feature <- 0 until numFeaturesPerGroup
    } yield {
      val name = s"my_group_$group"
      val featureName = s"my_feature_$feature"
      val tileKey = new TileSeriesKey()
      tileKey.setNodeName(join)
      tileKey.setGroupName(name)
      tileKey.setColumn(featureName)

      val tileDriftSeries = new TileDriftSeries()
      tileDriftSeries.setKey(tileKey)

      val timestamps = (startTs until endTs by (Duration(1, TimeUnit.HOURS).toMillis)).toList.map(JLong.valueOf(_)).asJava
      // if feature name ends in an even digit we consider it continuous and generate mock data accordingly
      // else we generate mock data for a categorical feature
      val isNumeric = if (feature % 2 == 0) true else false
      val percentileDrifts = if (isNumeric) List.fill(timestamps.size())(JDouble.valueOf(0.12)).asJava else List.fill[JDouble](timestamps.size())(null).asJava
      val histogramDrifts = if (isNumeric) List.fill[JDouble](timestamps.size())(null).asJava else List.fill(timestamps.size())(JDouble.valueOf(0.23)).asJava
      val nullRationChangePercents = List.fill(timestamps.size())(JDouble.valueOf(0.25)).asJava
      tileDriftSeries.setTimestamps(timestamps)
      tileDriftSeries.setPercentileDriftSeries(percentileDrifts)
      tileDriftSeries.setNullRatioChangePercentSeries(nullRationChangePercents)
      tileDriftSeries.setHistogramDriftSeries(histogramDrifts)
    }
    Success(Future.successful(result))
  }

  private def generateSummarySeries(startTs: Long, endTs: Long, join: String, groupBy: String, featureName: String, metric: Metric, isNumeric: Boolean): Try[Future[Seq[TileSummarySeries]]] = {
    val tileKey = new TileSeriesKey()
    tileKey.setNodeName(join)
    tileKey.setGroupName(groupBy)
    tileKey.setNodeName(join)
    tileKey.setColumn(featureName)

    val timestamps = (startTs until endTs by (Duration(1, TimeUnit.HOURS).toMillis)).toList.map(JLong.valueOf(_))
    val tileSummarySeries = new TileSummarySeries()
    tileSummarySeries.setKey(tileKey)
    tileSummarySeries.setTimestamps(timestamps.asJava)

    if (metric == NullMetric) {
      tileSummarySeries.setNullCount(List.fill(timestamps.length)(JLong.valueOf(1)).asJava)
    } else {
      if (isNumeric) {
        val percentileList = timestamps.map {
          _ =>
            List.fill(DriftStore.breaks(20).length)(JDouble.valueOf(0.12)).asJava
        }.asJava
        tileSummarySeries.setPercentiles(percentileList)
      } else {
        val histogramMap = mockCategories.map {
          category =>
            category -> List.fill(timestamps.length)(JLong.valueOf(1)).asJava
        }.toMap.asJava
        tileSummarySeries.setHistogram(histogramMap)
      }
    }

    Success(Future.successful(Seq(tileSummarySeries)))
  }
}
