package controllers

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import model._
import org.scalatest.EitherValues
import org.scalatestplus.play._
import play.api.http.Status.BAD_REQUEST
import play.api.http.Status.OK
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class TimeSeriesControllerSpec extends PlaySpec with Results with EitherValues {

  // Create a stub ControllerComponents
  val stubCC: ControllerComponents = stubControllerComponents()

  val controller = new TimeSeriesController(stubCC)

  "TimeSeriesController's model ts lookup" should {

    "send 400 on an invalid time offset" in {
      val invalid1 = controller.fetchModel("id-123", 123L, 456L, "Xh", "psi").apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 = controller.fetchModel("id-123", 123L, 456L, "-10h", "psi").apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid algorithm" in {
      val invalid1 = controller.fetchModel("id-123", 123L, 456L, "10h", "meow").apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send valid results on a correctly formed model ts request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result = controller.fetchModel("id-123", startTs, endTs, "10h", "psi").apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val modelTSResponse: Either[Error, ModelTimeSeriesResponse] = decode[ModelTimeSeriesResponse](bodyText)
      modelTSResponse.isRight mustBe true
      val items = modelTSResponse.right.value.items
      items.length mustBe (Duration(endTs, TimeUnit.MILLISECONDS) - Duration(startTs, TimeUnit.MILLISECONDS)).toHours
    }
  }

  "TimeSeriesController's join ts lookup" should {

    "send 400 on an invalid metric choice" in {
      val invalid = controller.fetchJoin("my_join", 123L, 456L, "meow", "null", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
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

    "send valid results on a correctly formed model ts drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller.fetchJoin("my_join", startTs, endTs, "drift", "null", Some("10h"), Some("psi")).apply(FakeRequest())
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

    "send valid results on a correctly formed model ts skew lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller.fetchJoin("my_join", startTs, endTs, "skew", "null", None, None).apply(FakeRequest())
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
      val invalid =
        controller.fetchFeature("my_feature", 123L, 456L, "meow", "null", "raw", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid metric rollup" in {
      val invalid =
        controller.fetchFeature("my_feature", 123L, 456L, "drift", "woof", "raw", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid granularity" in {
      val invalid =
        controller.fetchFeature("my_feature", 123L, 456L, "drift", "null", "woof", None, None).apply(FakeRequest())
      status(invalid) mustBe BAD_REQUEST
    }

    "send 400 on an invalid time offset for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_feature", 123L, 456L, "drift", "null", "aggregates", Some("Xh"), Some("psi"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST

      val invalid2 =
        controller
          .fetchFeature("my_feature", 123L, 456L, "drift", "null", "aggregates", Some("-1h"), Some("psi"))
          .apply(FakeRequest())
      status(invalid2) mustBe BAD_REQUEST
    }

    "send 400 on an invalid algorithm for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_feature", 123L, 456L, "drift", "null", "aggregates", Some("10h"), Some("meow"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send 400 on an invalid granularity for drift metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_feature", 123L, 456L, "drift", "null", "raw", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send 400 on an invalid granularity for skew metric" in {
      val invalid1 =
        controller
          .fetchFeature("my_feature", 123L, 456L, "skew", "null", "aggregates", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(invalid1) mustBe BAD_REQUEST
    }

    "send valid results on a correctly formed numeric feature ts aggregate drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller
          .fetchFeature("my_feature_0", startTs, endTs, "drift", "null", "aggregates", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature_0"
      response.current.length mustBe response.baseline.length
      response.current.zip(response.baseline).foreach {
        case (current, baseline) =>
          current.ts mustBe baseline.ts
      }
    }

    "send valid results on a correctly formed categorical feature ts aggregate drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller
          .fetchFeature("my_feature_1", startTs, endTs, "drift", "null", "aggregates", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] = decode[ComparedFeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature_1"
      response.current.map(_.ts).toSet mustBe response.baseline.map(_.ts).toSet
      response.current.foreach(_.label.isEmpty mustBe false)
      response.baseline.foreach(_.label.isEmpty mustBe false)
    }

    "send valid results on a correctly formed feature ts percentile drift lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller
          .fetchFeature("my_feature", startTs, endTs, "drift", "null", "percentile", Some("10h"), Some("psi"))
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, FeatureTimeSeries] = decode[FeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature"
      response.points.nonEmpty mustBe true

      // expect one entry per percentile for each time series point
      val expectedLength = TimeSeriesController.mockGeneratedPercentiles.length * expectedHours(startTs, endTs)
      response.points.length mustBe expectedLength
    }

    "send valid results on a correctly formed feature ts raw skew lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller.fetchFeature("my_feature", startTs, endTs, "skew", "null", "raw", None, None).apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, ComparedFeatureTimeSeries] =
        decode[ComparedFeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature"
      response.baseline.nonEmpty mustBe true
      response.baseline.length mustBe response.current.length
      // we expect a skew distribution at a fixed time stamp
      response.baseline.foreach(p => p.ts mustBe startTs)
      response.current.foreach(p => p.ts mustBe startTs)
    }

    "send valid results on a correctly formed feature ts percentile skew lookup request" in {
      val startTs = 1725926400000L // 09/10/2024 00:00 UTC
      val endTs = 1726106400000L // 09/12/2024 02:00 UTC
      val result =
        controller
          .fetchFeature("my_feature", startTs, endTs, "skew", "null", "percentile", None, None)
          .apply(FakeRequest())
      status(result) mustBe OK
      val bodyText = contentAsString(result)
      val featureTSResponse: Either[Error, FeatureTimeSeries] = decode[FeatureTimeSeries](bodyText)
      featureTSResponse.isRight mustBe true
      val response = featureTSResponse.right.value
      response.feature mustBe "my_feature"
      response.points.nonEmpty mustBe true

      // expect one entry per percentile for each time series point
      val expectedLength = TimeSeriesController.mockGeneratedPercentiles.length * expectedHours(startTs, endTs)
      response.points.length mustBe expectedLength
    }
  }

  private def expectedHours(startTs: Long, endTs: Long): Long = {
    Duration(endTs - startTs, TimeUnit.MILLISECONDS).toHours
  }
}
