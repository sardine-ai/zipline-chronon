package ai.chronon.hub.handlers

import ai.chronon.hub.model.ComparedFeatureTimeSeries
import ai.chronon.hub.model.FeatureTimeSeries
import ai.chronon.hub.model.JoinTimeSeriesResponse
import ai.chronon.hub.model.Metric
import ai.chronon.hub.model.NullMetric
import ai.chronon.hub.model.ValuesMetric
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSeriesKey
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.stats.DriftStore
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.vertx.core.Handler
import io.vertx.core.MultiMap
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.ext.web.RequestBody
import io.vertx.ext.web.RoutingContext
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.scalatest.EitherValues

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

@RunWith(classOf[VertxUnitRunner])
class TimeSeriesHandlerTest extends EitherValues {

  @Mock var routingContext: RoutingContext = _
  @Mock var response: HttpServerResponse = _
  @Mock var requestBody: RequestBody = _
  @Mock var mockedStore: DriftStore = _

  val mockCategories: Seq[String] = Seq("a", "b", "c")

  var vertx: Vertx = _
  var joinDriftHandler: Handler[RoutingContext] = _
  var featureDriftHandler: Handler[RoutingContext] = _

  @Before
  def setUp(context: TestContext): Unit = {
    MockitoAnnotations.openMocks(this)
    vertx = Vertx.vertx
    joinDriftHandler = new TimeSeriesHandler(mockedStore).joinDriftHandler
    featureDriftHandler = new TimeSeriesHandler(mockedStore).featureDriftHandler
    // Set up common routing context behavior
    when(routingContext.response).thenReturn(response)
    when(response.putHeader(anyString, anyString)).thenReturn(response)
    when(response.setStatusCode(anyInt)).thenReturn(response)
    when(routingContext.body).thenReturn(requestBody)
    when(mockedStore.executionContext).thenReturn(ExecutionContext.global)
  }

  @Test
  def test_joinTsLookup_Send400BadMetricChoice(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildJoinQueryParams(123L, 456L, "meow", "null", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  // we don't support skew atm
  @Test
  def test_joinTsLookup_Send400BadMetricChoice_Skew(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildJoinQueryParams(123L, 456L, "skew", "null", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_joinTsLookup_Send400BadMetricRollup(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildJoinQueryParams(123L, 456L, "drift", "woof", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_joinTsLookup_Send400InvalidTimeOffset(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildJoinQueryParams(123L, 456L, "drift", "null", Some("-1h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_joinTsLookup_Send400InvalidAlgorithm(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildJoinQueryParams(123L, 456L, "drift", "null", Some("10h"), Some("meow"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_joinTsLookup_Send5xxFailedDriftStoreLookup(context: TestContext): Unit = {
    val async = context.async

    when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any()))
      .thenReturn(Failure(new IllegalArgumentException("Some internal error")))

    val startTs = 1725926400000L // 09/10/2024 00:00 UTC
    val endTs = 1726106400000L // 09/12/2024 02:00 UTC
    val multiMap = buildJoinQueryParams(startTs, endTs, "drift", "null", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(500)
                     async.complete()
                   })
  }

  @Test
  def test_joinTsLookup_SendValidResultsOnCorrectlyFormedRequest(context: TestContext): Unit = {
    val async = context.async

    val startTs = 1725926400000L // 09/10/2024 00:00 UTC
    val endTs = 1726106400000L // 09/12/2024 02:00 UTC
    val mockedDriftStoreResponse = generateDriftSeries(startTs, endTs, "my_join", 2, 3)
    when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any())).thenReturn(mockedDriftStoreResponse)

    val multiMap = buildJoinQueryParams(startTs, endTs, "drift", "value", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("my_join")

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    joinDriftHandler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val bodyText = responseCaptor.getValue

        val modelTSResponse: Either[Error, JoinTimeSeriesResponse] = decode[JoinTimeSeriesResponse](bodyText)
        assertTrue(modelTSResponse.isRight)
        val tsResponse = modelTSResponse.right.value
        assertEquals(tsResponse.name, "my_join")
        assertTrue(tsResponse.items.nonEmpty)

        val expectedLength = expectedHours(startTs, endTs)
        tsResponse.items.foreach { grpByTs =>
          assertFalse(grpByTs.items.isEmpty)
          grpByTs.items.foreach(featureTs => assertEquals(featureTs.points.length, expectedLength))
        }

        async.complete()
      }
    )
  }

  @Test
  def test_featureTsLookup_Send400InvalidMetricChoice(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "meow", "null", "raw", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  // skew is unsupported atm
  @Test
  def test_featureTsLookup_Send400InvalidMetricChoice_Skew(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "skew", "null", "raw", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_Send400InvalidMetricRollup(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "drift", "woof", "raw", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_Send400InvalidGranularity(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "drift", "null", "woof", None, None)
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_Send400InvalidTimeOffset(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "drift", "null", "aggregates", Some("-1h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_Send400InvalidAlgorithm(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "drift", "null", "aggregates", Some("10h"), Some("meow"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_Send400InvalidRawGranularity(context: TestContext): Unit = {
    val async = context.async
    val multiMap = buildFeatureQueryParams(123L, 456L, "drift", "null", "raw", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def test_featureTsLookup_SendValidResultsOnCorrectlyFormedReq(context: TestContext): Unit = {
    val async = context.async

    val startTs = 1725926400000L // 09/10/2024 00:00 UTC
    val endTs = 1726106400000L // 09/12/2024 02:00 UTC

    val mockedDriftStoreResponse = generateDriftSeries(startTs, endTs, "my_join", 1, 1)
    when(mockedStore.getDriftSeries(any(), any(), any(), any(), any(), any())).thenReturn(mockedDriftStoreResponse)

    val multiMap = buildFeatureQueryParams(startTs, endTs, "drift", "null", "aggregates", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature_0")

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val bodyText = responseCaptor.getValue

        val featureTSResponse: Either[Error, FeatureTimeSeries] = decode[FeatureTimeSeries](bodyText)
        assertTrue(featureTSResponse.isRight)
        val tsResponse = featureTSResponse.right.value
        assertEquals(tsResponse.feature, "my_feature_0")
        val expectedLength = expectedHours(startTs, endTs)
        assertEquals(tsResponse.points.length, expectedLength)

        async.complete()
      }
    )
  }

  @Test
  def test_featureTsLookup_SendValidResultsNumericFeatureTs(context: TestContext): Unit = {
    val async = context.async

    val startTs = 1725926400000L // 09/10/2024 00:00 UTC
    val endTs = 1726106400000L // 09/12/2024 02:00 UTC

    val mockedSummarySeriesResponseA =
      generateSummarySeries(startTs, endTs, "my_join", "my_groupby", "my_feature", ValuesMetric, true)
    val offset = Duration.apply(7, TimeUnit.DAYS)
    val mockedSummarySeriesResponseB =
      generateSummarySeries(startTs - offset.toMillis,
                            endTs - offset.toMillis,
                            "my_join",
                            "my_groupby",
                            "my_feature",
                            ValuesMetric,
                            true)
    when(mockedStore.getSummarySeries(any(), any(), any(), any()))
      .thenReturn(mockedSummarySeriesResponseA, mockedSummarySeriesResponseB)

    val multiMap = buildFeatureQueryParams(startTs, endTs, "drift", "value", "percentile", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val bodyText = responseCaptor.getValue

        val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
        assertTrue(featureTSResponse.isRight)
        val tsResponse = featureTSResponse.right.value
        assertEquals(tsResponse.feature, "my_feature")

        assertEquals(tsResponse.current.length, tsResponse.baseline.length)
        tsResponse.current.zip(tsResponse.baseline).foreach {
          case (current, baseline) =>
            assertEquals((current.ts - baseline.ts), offset.toMillis)
        }

        // expect one entry per percentile for each time series point
        val expectedLength = DriftStore.breaks(20).length * expectedHours(startTs, endTs)
        assertEquals(tsResponse.current.length, expectedLength)
        assertEquals(tsResponse.baseline.length, expectedLength)

        async.complete()
      }
    )
  }

  @Test
  def test_featureTsLookup_SendValidResultsCategoricalFeatureTs(context: TestContext): Unit = {
    val async = context.async

    val startTs = 1725926400000L // 09/10/2024 00:00 UTC
    val endTs = 1726106400000L // 09/12/2024 02:00 UTC

    val mockedSummarySeriesResponseA =
      generateSummarySeries(startTs, endTs, "my_join", "my_groupby", "my_feature", ValuesMetric, false)
    val offset = Duration.apply(7, TimeUnit.DAYS)
    val mockedSummarySeriesResponseB =
      generateSummarySeries(startTs - offset.toMillis,
                            endTs - offset.toMillis,
                            "my_join",
                            "my_groupby",
                            "my_feature",
                            ValuesMetric,
                            false)
    when(mockedStore.getSummarySeries(any(), any(), any(), any()))
      .thenReturn(mockedSummarySeriesResponseA, mockedSummarySeriesResponseB)

    val multiMap = buildFeatureQueryParams(startTs, endTs, "drift", "value", "percentile", Some("10h"), Some("psi"))
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("join")).thenReturn("my_join")
    when(routingContext.pathParam("name")).thenReturn("my_feature")

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    featureDriftHandler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val bodyText = responseCaptor.getValue

        val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
        assertTrue(featureTSResponse.isRight)
        val tsResponse = featureTSResponse.right.value
        assertEquals(tsResponse.feature, "my_feature")
        assertEquals(tsResponse.current.length, tsResponse.baseline.length)

        // expect one entry per category for each time series point
        val expectedLength = mockCategories.length * expectedHours(startTs, endTs)
        assertEquals(tsResponse.current.length, expectedLength)

        tsResponse.current.zip(tsResponse.baseline).foreach {
          case (current, baseline) =>
            assertEquals((current.ts - baseline.ts), offset.toMillis)
            assertFalse(current.label.isEmpty)
            assertFalse(baseline.label.isEmpty)
        }

        async.complete()
      }
    )
  }

  private def buildJoinQueryParams(startTs: Long,
                                   endTs: Long,
                                   metricType: String,
                                   metrics: String,
                                   offset: Option[String],
                                   algorithm: Option[String]): MultiMap = {
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("startTs", startTs.toString)
    multiMap.add("endTs", endTs.toString)
    multiMap.add("metricType", metricType)
    multiMap.add("metrics", metrics)
    if (offset.isDefined)
      multiMap.add("offset", offset.get)
    if (algorithm.isDefined)
      multiMap.add("algorithm", algorithm.get)
    multiMap
  }

  private def buildFeatureQueryParams(startTs: Long,
                                      endTs: Long,
                                      metricType: String,
                                      metrics: String,
                                      granularity: String,
                                      offset: Option[String],
                                      algorithm: Option[String]): MultiMap = {
    val baseMultiMap = buildJoinQueryParams(startTs, endTs, metricType, metrics, offset, algorithm)
    baseMultiMap.add("granularity", granularity)
    baseMultiMap
  }

  private def expectedHours(startTs: Long, endTs: Long): Long = {
    Duration(endTs - startTs, TimeUnit.MILLISECONDS).toHours
  }

  private def generateDriftSeries(startTs: Long,
                                  endTs: Long,
                                  join: String,
                                  numGroups: Int,
                                  numFeaturesPerGroup: Int): Try[Future[Seq[TileDriftSeries]]] = {
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

      val timestamps =
        (startTs until endTs by (Duration(1, TimeUnit.HOURS).toMillis)).toList.map(JLong.valueOf(_)).asJava
      // if feature name ends in an even digit we consider it continuous and generate mock data accordingly
      // else we generate mock data for a categorical feature
      val isNumeric = if (feature % 2 == 0) true else false
      val percentileDrifts =
        if (isNumeric) List.fill(timestamps.size())(JDouble.valueOf(0.12)).asJava
        else List.fill[JDouble](timestamps.size())(null).asJava
      val histogramDrifts =
        if (isNumeric) List.fill[JDouble](timestamps.size())(null).asJava
        else List.fill(timestamps.size())(JDouble.valueOf(0.23)).asJava
      val nullRationChangePercents = List.fill(timestamps.size())(JDouble.valueOf(0.25)).asJava
      tileDriftSeries.setTimestamps(timestamps)
      tileDriftSeries.setPercentileDriftSeries(percentileDrifts)
      tileDriftSeries.setNullRatioChangePercentSeries(nullRationChangePercents)
      tileDriftSeries.setHistogramDriftSeries(histogramDrifts)
    }
    Success(Future.successful(result))
  }

  private def generateSummarySeries(startTs: Long,
                                    endTs: Long,
                                    join: String,
                                    groupBy: String,
                                    featureName: String,
                                    metric: Metric,
                                    isNumeric: Boolean): Try[Future[Seq[TileSummarySeries]]] = {
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
        val percentileList = DriftStore
          .breaks(20)
          .map { _ =>
            List.fill(timestamps.length)(JDouble.valueOf(0.12)).asJava
          }
          .asJava
        tileSummarySeries.setPercentiles(percentileList)
      } else {
        val histogramMap = mockCategories
          .map { category =>
            category -> List.fill(timestamps.length)(JLong.valueOf(1)).asJava
          }
          .toMap
          .asJava
        tileSummarySeries.setHistogram(histogramMap)
      }
    }

    Success(Future.successful(Seq(tileSummarySeries)))
  }
}
