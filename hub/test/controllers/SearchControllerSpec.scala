package controllers

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import model.ListModelResponse
import org.scalatest.EitherValues
import org.scalatestplus.play._
import play.api.http.Status.BAD_REQUEST
import play.api.http.Status.OK
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._

class SearchControllerSpec extends PlaySpec with Results with EitherValues {

  // Create a stub ControllerComponents
  val stubCC: ControllerComponents = stubControllerComponents()

  val controller = new SearchController(stubCC)

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
      val result = controller.search("1", None, None).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listModelResponse: Either[Error, ListModelResponse] = decode[ListModelResponse](bodyText)
      val items = listModelResponse.value.items
      items.length mustBe controller.defaultLimit
      items.map(_.id.toInt).toSet mustBe Set(1, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    }

    "send results in a paginated fashion correctly" in {
      val startOffset = 25
      val number = 20
      val result = controller.search("test", Some(startOffset), Some(number)).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val listModelResponse: Either[Error, ListModelResponse] = decode[ListModelResponse](bodyText)
      val items = listModelResponse.value.items
      items.length mustBe number
      items.map(_.id.toInt).toSet mustBe (startOffset until startOffset + number).toSet
    }
  }
}
