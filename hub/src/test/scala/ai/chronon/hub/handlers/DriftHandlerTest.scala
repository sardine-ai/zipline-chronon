package ai.chronon.hub.handlers

import ai.chronon.api.Window
import ai.chronon.observability._
import ai.chronon.online.stats.DriftStore
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito._
import org.mockito.MockitoAnnotations

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Success

@RunWith(classOf[VertxUnitRunner])
class DriftHandlerTest {

  @Mock private var mockedDriftStore: DriftStore = _
  private var handler: DriftHandler = _

  private val testJoinName = "test_join"
  private val testColumnName = "test_column"
  private val baseTimestamp = System.currentTimeMillis()

  @Before
  def setUp(): Unit = {
    MockitoAnnotations.openMocks(this)
    handler = new DriftHandler(mockedDriftStore)
  }

  private def createMockDriftSeries(timestamp: Long): TileDriftSeries = {
    val series = new TileDriftSeries()
    series.setTimestamps(List(timestamp: java.lang.Long).asJava)
    series.setPercentileDriftSeries(List(0.1: java.lang.Double).asJava)
    series.setHistogramDriftSeries(List(0.2: java.lang.Double).asJava)
    series.setCountChangePercentSeries(List(0.3: java.lang.Double).asJava)
    
    val key = new TileSeriesKey()
    key.setColumn(testColumnName)
    series.setKey(key)
    series
  }

  private def createMockSummarySeries(timestamp: Long): TileSummarySeries = {
    val series = new TileSummarySeries()
    series.setTimestamps(List(timestamp: java.lang.Long).asJava)
    series.setPercentiles(List(List(0.1: java.lang.Double, 0.5: java.lang.Double, 0.9: java.lang.Double).asJava).asJava)
    series.setCount(List(100L: java.lang.Long).asJava)
    
    val key = new TileSeriesKey()
    key.setColumn(testColumnName)
    series.setKey(key)
    series
  }

  @Test
  def testGetDriftWithHourOffset(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setColumnName(testColumnName)
    request.setStartTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    request.setEndTs(baseTimestamp)
    request.setOffset("24h")
    request.setAlgorithm(DriftMetric.JENSEN_SHANNON)

    val mockDriftSeries = List(createMockDriftSeries(baseTimestamp))
    when(mockedDriftStore.getDriftSeries(
      anyString(),
      any[DriftMetric],
      any[Window],
      anyLong(),
      anyLong(),
      any[Option[String]]
    )).thenReturn(Success(Future.successful(mockDriftSeries)))

    val response = handler.getJoinDrift(request)
    assertNotNull(response.getDriftSeries)
    assertEquals(1, response.getDriftSeries.size())
    assertEquals(testColumnName, response.getDriftSeries.get(0).getKey.getColumn)
  }

  @Test
  def testGetDriftWithDayOffset(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setColumnName(testColumnName)
    request.setStartTs(baseTimestamp - TimeUnit.DAYS.toMillis(7))
    request.setEndTs(baseTimestamp)
    request.setOffset("7d")
    request.setAlgorithm(DriftMetric.JENSEN_SHANNON)

    val mockDriftSeries = List(createMockDriftSeries(baseTimestamp))
    when(mockedDriftStore.getDriftSeries(
      anyString(),
      any[DriftMetric],
      any[Window],
      anyLong(),
      anyLong(),
      any[Option[String]]
    )).thenReturn(Success(Future.successful(mockDriftSeries)))

    val response = handler.getJoinDrift(request)
    assertNotNull(response.getDriftSeries)
    assertEquals(1, response.getDriftSeries.size())
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGetDriftWithInvalidOffset(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setOffset("invalid")
    request.setStartTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    request.setEndTs(baseTimestamp)
    
    handler.getJoinDrift(request)
  }

  @Test
  def testGetSummary(): Unit = {
    val request = new JoinSummaryRequest()
    request.setName(testJoinName)
    request.setColumnName(testColumnName)
    request.setStartTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    request.setEndTs(baseTimestamp)

    val mockSummarySeries = List(createMockSummarySeries(baseTimestamp))
    when(mockedDriftStore.getSummarySeries(
      anyString(),
      anyLong(),
      anyLong(),
      any[Option[String]]
    )).thenReturn(Success(Future.successful(mockSummarySeries)))

    val response = handler.getColumnSummary(request)
    assertNotNull(response)
    assertNotNull(response.getTimestamps)
    assertEquals(1, response.getTimestamps.size())
    assertEquals(testColumnName, response.getKey.getColumn)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGetDriftWithEndTimeBeforeStartTime(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setStartTs(baseTimestamp)
    request.setEndTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    // We don't need to set offset or algorithm since the timestamp validation should happen first
    
    handler.getJoinDrift(request)
  }

  @Test
  def testGetDriftWithDefaultAlgorithm(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setColumnName(testColumnName)
    request.setStartTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    request.setEndTs(baseTimestamp)
    request.setOffset("24h")
    // Not setting algorithm should use default

    val mockDriftSeries = List(createMockDriftSeries(baseTimestamp))
    when(mockedDriftStore.getDriftSeries(
      anyString(),
      any[DriftMetric],
      any[Window],
      anyLong(),
      anyLong(),
      any[Option[String]]
    )).thenReturn(Success(Future.successful(mockDriftSeries)))

    val response = handler.getJoinDrift(request)
    assertNotNull(response.getDriftSeries)
    assertEquals(1, response.getDriftSeries.size())
  }

  @Test
  def testGetDriftWithNullColumnName(): Unit = {
    val request = new JoinDriftRequest()
    request.setName(testJoinName)
    request.setStartTs(baseTimestamp - TimeUnit.HOURS.toMillis(24))
    request.setEndTs(baseTimestamp)
    request.setOffset("24h")
    // Not setting columnName

    val mockDriftSeries = List(createMockDriftSeries(baseTimestamp))
    when(mockedDriftStore.getDriftSeries(
      anyString(),
      any[DriftMetric],
      any[Window],
      anyLong(),
      anyLong(),
      any[Option[String]]
    )).thenReturn(Success(Future.successful(mockDriftSeries)))

    val response = handler.getJoinDrift(request)
    assertNotNull(response.getDriftSeries)
    assertEquals(1, response.getDriftSeries.size())
  }
}