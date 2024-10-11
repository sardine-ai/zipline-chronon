package controllers

import controllers.MockDataService.mockModelRegistry
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import model.ListModelResponse
import org.mockito.Mockito.{mock, when}
import org.scalatest.EitherValues
import org.scalatestplus.play._
import play.api.http.Status.BAD_REQUEST
import play.api.http.Status.OK
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._
import store.DynamoDBMonitoringStore

class SearchControllerSpec extends PlaySpec with Results with EitherValues {

  // Create a stub ControllerComponents
  val stubCC: ControllerComponents = stubControllerComponents()
  // Create a mocked DynDB store
  val mockedStore = mock(classOf[DynamoDBMonitoringStore])

  val controller = new SearchController(stubCC, mockedStore)

  "SearchController" should {

    "send 400 on a bad offset" in {
      val result = controller.search("foo", Some(-1), Some(10)).apply(FakeRequest())
      status(result) mustBe BAD_REQUEST
    }

    "send 400 on a bad limit" in {
      val result = controller.search("foo", Some(10), Some(-2)).apply(FakeRequest())
      status(result) mustBe BAD_REQUEST
    }

    "send valid results on a correctly formed request" in {
      when(mockedStore.getModels).thenReturn(mockModelRegistry)

      val result = controller.search("1", None, None).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listModelResponse: Either[Error, ListModelResponse] = decode[ListModelResponse](bodyText)
      val items = listModelResponse.right.value.items
      items.length mustBe controller.defaultLimit
      items.map(_.name.toInt).toSet mustBe Set(1, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    }

    "send results in a paginated fashion correctly" in {
      when(mockedStore.getModels).thenReturn(mockModelRegistry)

      val startOffset = 3
      val number = 6
      val result = controller.search("1", Some(startOffset), Some(number)).apply(FakeRequest())
      // we have names: 0, 1, 2, .. 99
      // our result should give us: 1, 10, 11, 12, .. 19, 21, 31, .. 91
      val expected = Set(12, 13, 14, 15, 16, 17)
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listModelResponse: Either[Error, ListModelResponse] = decode[ListModelResponse](bodyText)
      val items = listModelResponse.right.value.items
      items.length mustBe number
      items.map(_.name.toInt).toSet mustBe expected
    }
  }
}
