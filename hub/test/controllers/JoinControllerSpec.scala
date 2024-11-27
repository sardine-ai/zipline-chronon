package controllers

import controllers.MockJoinService.mockJoinRegistry
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import model.Join
import model.ListJoinResponse
import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatestplus.play._
import play.api.http.Status.BAD_REQUEST
import play.api.http.Status.OK
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._
import store.MonitoringModelStore

class JoinControllerSpec extends PlaySpec with Results with EitherValues {

  // Create a stub ControllerComponents
  val stubCC: ControllerComponents = stubControllerComponents()
  // Create a mocked DynDB store
  val mockedStore: MonitoringModelStore = mock(classOf[MonitoringModelStore])

  val controller = new JoinController(stubCC, mockedStore)

  "JoinController" should {

    "send 400 on a bad offset" in {
      val result = controller.list(Some(-1), Some(10)).apply(FakeRequest())
      status(result) mustBe BAD_REQUEST
    }

    "send 400 on a bad limit" in {
      val result = controller.list(Some(10), Some(-2)).apply(FakeRequest())
      status(result) mustBe BAD_REQUEST
    }

    "send 404 on missing join" in {
      when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

      val result = controller.get("fake_join").apply(FakeRequest())
      status(result) mustBe NOT_FOUND
    }

    "send valid results on a correctly formed request" in {
      when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

      val result = controller.list(None, None).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listJoinResponse: Either[Error, ListJoinResponse] = decode[ListJoinResponse](bodyText)
      val items = listJoinResponse.right.value.items
      items.length mustBe controller.defaultLimit
      items.map(_.name.toInt).toSet mustBe (0 until 10).toSet
    }

    "send results in a paginated fashion correctly" in {
      when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

      val startOffset = 25
      val number = 20
      val result = controller.list(Some(startOffset), Some(number)).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listJoinResponse: Either[Error, ListJoinResponse] = decode[ListJoinResponse](bodyText)
      val items = listJoinResponse.right.value.items
      items.length mustBe number
      items.map(_.name.toInt).toSet mustBe (startOffset until startOffset + number).toSet
    }

    "send valid join object on specific join lookup" in {
      when(mockedStore.getJoins).thenReturn(mockJoinRegistry)

      val result = controller.get("10").apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val joinResponse: Either[Error, Join] = decode[Join](bodyText)
      joinResponse.right.value.name mustBe "10"
    }
  }
}
