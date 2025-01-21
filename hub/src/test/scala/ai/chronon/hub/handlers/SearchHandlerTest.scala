package ai.chronon.hub.handlers

import ai.chronon.hub.handlers.MockJoinService.mockJoinRegistry
import ai.chronon.hub.model.GroupBy
import ai.chronon.hub.model.Join
import ai.chronon.hub.model.SearchJoinResponse
import ai.chronon.hub.store.MonitoringModelStore
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
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
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.scalatest.EitherValues

@RunWith(classOf[VertxUnitRunner])
class SearchHandlerTest extends EitherValues {

  @Mock var routingContext: RoutingContext = _
  @Mock var response: HttpServerResponse = _
  @Mock var requestBody: RequestBody = _
  @Mock var mockedStore: MonitoringModelStore = _

  var vertx: Vertx = _
  var handler: SearchHandler = _

  @Before
  def setUp(context: TestContext): Unit = {
    MockitoAnnotations.openMocks(this)
    vertx = Vertx.vertx
    handler = new SearchHandler(mockedStore)
    // Set up common routing context behavior
    when(routingContext.response).thenReturn(response)
    when(response.putHeader(anyString, anyString)).thenReturn(response)
    when(response.setStatusCode(anyInt)).thenReturn(response)
    when(routingContext.body).thenReturn(requestBody)
  }

  @Test
  def testSend400BadOffset(context: TestContext): Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("term", "foo")
    multiMap.add("offset", "-1")
    multiMap.add("limit", "10")
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Trigger call// Trigger call
    handler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def testSend400BadLimit(context: TestContext): Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("term", "foo")
    multiMap.add("offset", "10")
    multiMap.add("limit", "-1")
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Trigger call// Trigger call
    handler.handle(routingContext)
    vertx.setTimer(1000,
                   _ => {
                     verify(response).setStatusCode(400)
                     async.complete()
                   })
  }

  @Test
  def testSendValidResults(context: TestContext): Unit = {
    val async = context.async
    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("term", "1")
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    handler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val jsonResponse = responseCaptor.getValue

        val listResponse: Either[Error, SearchJoinResponse] = decode[SearchJoinResponse](jsonResponse)
        val items = listResponse.right.value.items
        assertEquals(items.length, handler.defaultLimit)
        assertEquals(items.map(_.name.toInt).toSet, Set(1, 10, 11, 12, 13, 14, 15, 16, 17, 18))

        async.complete()
      }
    )
  }

  @Test
  def testSendPaginatedResultsCorrectly(context: TestContext): Unit = {
    val async = context.async
    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

    val multiMap = MultiMap.caseInsensitiveMultiMap
    val number = 6
    val startOffset = 3
    multiMap.add("term", "1")
    multiMap.add("offset", startOffset.toString)
    multiMap.add("limit", number.toString)
    when(routingContext.queryParams()).thenReturn(multiMap)

    // we have names: 0, 1, 2, .. 99
    // our result should give us: 1, 10, 11, 12, .. 19, 21, 31, .. 91
    val expected = Set(12, 13, 14, 15, 16, 17)

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    handler.handle(routingContext)
    vertx.setTimer(
      1000,
      _ => {
        verify(response).setStatusCode(200)
        verify(response).putHeader("content-type", "application/json")
        verify(response).end(responseCaptor.capture)
        val jsonResponse = responseCaptor.getValue

        val listResponse: Either[Error, SearchJoinResponse] = decode[SearchJoinResponse](jsonResponse)
        val items = listResponse.right.value.items
        assertEquals(items.length, number)
        assertEquals(items.map(_.name.toInt).toSet, expected)

        async.complete()
      }
    )
  }
}

object MockJoinService {
  def generateMockJoin(id: String): Join = {
    val groupBys = Seq(GroupBy("my_groupBy", Seq("g1", "g2")))
    Join(id, Seq("ext_f1", "ext_f2", "d_1", "d2"), groupBys, true, true, Some("my_team"))
  }

  val mockJoinRegistry: Seq[Join] = (0 until 100).map(i => generateMockJoin(i.toString))
}
