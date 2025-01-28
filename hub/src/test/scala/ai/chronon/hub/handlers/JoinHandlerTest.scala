package ai.chronon.hub.handlers

import ai.chronon.hub.handlers.MockJoinService.mockJoinRegistry
import ai.chronon.hub.model.Join
import ai.chronon.hub.model.ListJoinResponse
import ai.chronon.hub.store.MonitoringModelStore
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
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.scalatest.EitherValues

@RunWith(classOf[VertxUnitRunner])
class JoinHandlerTest extends EitherValues {

  @Mock var routingContext: RoutingContext = _
  @Mock var response: HttpServerResponse = _
  @Mock var requestBody: RequestBody = _
  @Mock var mockedStore: MonitoringModelStore = _

  var vertx: Vertx = _
  var listHandler: Handler[RoutingContext] = _
  var getHandler: Handler[RoutingContext] = _

  @Before
  def setUp(context: TestContext): Unit = {
    MockitoAnnotations.openMocks(this)
    vertx = Vertx.vertx
    listHandler = new JoinsHandler(mockedStore).listHandler
    getHandler = new JoinsHandler(mockedStore).getHandler
    // Set up common routing context behavior
    when(routingContext.response).thenReturn(response)
    when(response.putHeader(anyString, anyString)).thenReturn(response)
    when(response.setStatusCode(anyInt)).thenReturn(response)
    when(routingContext.body).thenReturn(requestBody)
  }

  @Test
  def testSend400BadOffset(context: TestContext) : Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("offset", "-1")
    multiMap.add("limit", "10")
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Trigger call// Trigger call
    listHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(400)
      async.complete()
    })
  }

  @Test
  def testSend400BadLimit(context: TestContext) : Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("offset", "10")
    multiMap.add("limit", "-1")
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Trigger call// Trigger call
    listHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(400)
      async.complete()
    })
  }

  @Test
  def testSend404MissingJoin(context: TestContext) : Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("offset", "10")
    multiMap.add("limit", "-1")
    when(routingContext.queryParams()).thenReturn(multiMap)
    when(routingContext.pathParam("name")).thenReturn("fake_join")
    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

    // Trigger call// Trigger call
    getHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(404)
      async.complete()
    })
  }

  @Test
  def testSendValidResults(context: TestContext) : Unit = {
    val async = context.async
    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)
    val multiMap = MultiMap.caseInsensitiveMultiMap
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    listHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(200)
      verify(response).putHeader("content-type", "application/json")
      verify(response).end(responseCaptor.capture)
      val jsonResponse = responseCaptor.getValue

      val listResponse: Either[Error, ListJoinResponse] = decode[ListJoinResponse](jsonResponse)
      val items = listResponse.right.value.items
      assertEquals(items.length, new JoinsHandler(mockedStore).defaultLimit)
      assertEquals(items.map(_.name.toInt).toSet, (0 until 10).toSet)

      async.complete()
    })
  }

  @Test
  def testSendPaginatedResultsCorrectly(context: TestContext) : Unit = {
    val async = context.async
    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

    val multiMap = MultiMap.caseInsensitiveMultiMap
    val number = 10
    val startOffset = 25
    multiMap.add("offset", startOffset.toString)
    multiMap.add("limit", number.toString)
    when(routingContext.queryParams()).thenReturn(multiMap)

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    listHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(200)
      verify(response).putHeader("content-type", "application/json")
      verify(response).end(responseCaptor.capture)
      val jsonResponse = responseCaptor.getValue

      val listResponse: Either[Error, ListJoinResponse] = decode[ListJoinResponse](jsonResponse)
      val items = listResponse.right.value.items
      assertEquals(items.length, number)
      assertEquals(items.map(_.name.toInt).toSet, (startOffset until startOffset + number).toSet)

      async.complete()
    })
  }

  @Test
  def testSendValidJoinOnLookup(context: TestContext) : Unit = {
    val async = context.async
    val multiMap = MultiMap.caseInsensitiveMultiMap
    multiMap.add("offset", "10")
    multiMap.add("limit", "-1")
    when(routingContext.pathParam("name")).thenReturn("10")

    when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

    // Capture the response that will be sent
    val responseCaptor = ArgumentCaptor.forClass(classOf[String])

    // Trigger call// Trigger call
    getHandler.handle(routingContext)
    vertx.setTimer(1000, _ => {
      verify(response).setStatusCode(200)
      verify(response).putHeader("content-type", "application/json")
      verify(response).end(responseCaptor.capture)
      val jsonResponse = responseCaptor.getValue

      val joinResponse: Either[Error, Join] = decode[Join](jsonResponse)
      assertEquals(joinResponse.right.value.name,  "10")

      async.complete()
    })
  }

}
