package controllers
import io.circe.generic.auto._
import io.circe.syntax._
import model._
import play.api.mvc._

import javax.inject._
import scala.concurrent.duration._
import scala.util.Random

/**
  * Controller that serves various time series endpoints at the model, join and feature level
  */
@Singleton
class TimeSeriesController @Inject() (val controllerComponents: ControllerComponents) extends BaseController {

  import TimeSeriesController._

  /**
    * Helps retrieve a model performance drift time series. Time series is retrieved between the start and end ts.
    * The offset is used to compute the distribution to compare against (we compare current time range with the same
    * sized time range starting offset time period prior).
    */
  def fetchModel(id: String, startTs: Long, endTs: Long, offset: String, algorithm: String): Action[AnyContent] =
    doFetchModel(id, startTs, endTs, offset, algorithm)

  /**
    * Helps retrieve a model time series with the data sliced based on the relevant slice (identified by sliceId)
    */
  def fetchModelSlice(id: String,
                      sliceId: String,
                      startTs: Long,
                      endTs: Long,
                      offset: String,
                      algorithm: String): Action[AnyContent] =
    doFetchModel(id, startTs, endTs, offset, algorithm, Some(sliceId))

  /**
    * Helps retrieve a time series (drift or skew) for each of the features that are part of a join. Time series is
    * retrieved between the start and end ts. If the metric type is for drift, the offset is used to compute the
    * distribution to compare against (we compare current time range with the same sized time range starting offset time
    * period prior). We compute either the null drift / skew or value's drift or skew based on the
    * metric choice.
    */
  def fetchJoin(name: String,
                startTs: Long,
                endTs: Long,
                metricType: String,
                metrics: String,
                offset: Option[String],
                algorithm: Option[String]): Action[AnyContent] =
    doFetchJoin(name, startTs, endTs, metricType, metrics, None, offset, algorithm)

  /**
    * Helps retrieve a time series (drift or skew) for each of the features that are part of a join. The data is sliced
    * based on the configured slice (looked up by sliceId)
    */
  def fetchJoinSlice(name: String,
                     sliceId: String,
                     startTs: Long,
                     endTs: Long,
                     metricType: String,
                     metrics: String,
                     offset: Option[String],
                     algorithm: Option[String]): Action[AnyContent] =
    doFetchJoin(name, startTs, endTs, metricType, metrics, Some(sliceId), offset, algorithm)

  /**
    * Helps retrieve a time series (drift or skew) for a given feature. Time series is
    * retrieved between the start and end ts. Choice of granularity (raw, aggregate, percentiles) along with the
    * metric type (drift / skew) dictates the shape of the returned time series.
    */
  def fetchFeature(name: String,
                   startTs: Long,
                   endTs: Long,
                   metricType: String,
                   metrics: String,
                   granularity: String,
                   offset: Option[String],
                   algorithm: Option[String]): Action[AnyContent] =
    doFetchFeature(name, startTs, endTs, metricType, metrics, None, granularity, offset, algorithm)

  /**
    * Helps retrieve a time series (drift or skew) for a given feature. The data is sliced based on the configured slice
    * (looked up by sliceId)
    */
  def fetchFeatureSlice(name: String,
                        sliceId: String,
                        startTs: Long,
                        endTs: Long,
                        metricType: String,
                        metrics: String,
                        granularity: String,
                        offset: Option[String],
                        algorithm: Option[String]): Action[AnyContent] =
    doFetchFeature(name, startTs, endTs, metricType, metrics, Some(sliceId), granularity, offset, algorithm)

  private def doFetchModel(id: String,
                           startTs: Long,
                           endTs: Long,
                           offset: String,
                           algorithm: String,
                           sliceId: Option[String] = None): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      (parseOffset(Some(offset)), parseAlgorithm(Some(algorithm))) match {
        case (None, _)          => BadRequest(s"Unable to parse offset - $offset")
        case (_, None)          => BadRequest("Invalid drift algorithm. Expect PSI or KL")
        case (Some(_), Some(_)) =>
          // TODO: Use parsedOffset and parsedAlgorithm when ready
          val mockTSData = ModelTimeSeriesResponse(id, generateMockTimeSeriesPoints(startTs, endTs))
          Ok(mockTSData.asJson.noSpaces)
      }
    }

  private def doFetchJoin(name: String,
                          startTs: Long,
                          endTs: Long,
                          metricType: String,
                          metrics: String,
                          slice: Option[String],
                          offset: Option[String],
                          algorithm: Option[String]): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      val metricChoice = parseMetricChoice(Some(metricType))
      val metricRollup = parseMetricRollup(Some(metrics))

      (metricChoice, metricRollup) match {
        case (None, _)                   => BadRequest("Invalid metric choice. Expect drift / skew")
        case (_, None)                   => BadRequest("Invalid metric rollup. Expect null / value")
        case (Some(Drift), Some(rollup)) => doFetchJoinDrift(name, startTs, endTs, rollup, slice, offset, algorithm)
        case (Some(Skew), Some(rollup))  => doFetchJoinSkew(name, startTs, endTs, rollup, slice)
      }
    }

  private def doFetchJoinDrift(name: String,
                               startTs: Long,
                               endTs: Long,
                               metric: Metric,
                               sliceId: Option[String],
                               offset: Option[String],
                               algorithm: Option[String]): Result = {

    (parseOffset(offset), parseAlgorithm(algorithm)) match {
      case (None, _)          => BadRequest(s"Unable to parse offset - $offset")
      case (_, None)          => BadRequest("Invalid drift algorithm. Expect PSI or KL")
      case (Some(_), Some(_)) =>
        // TODO: Use parsedOffset and parsedAlgorithm when ready
        val mockGroupBys = generateMockGroupBys(3)
        val groupByTimeSeries = mockGroupBys.map { g =>
          val mockFeatures = generateMockFeatures(g, 10)
          val featureTS = mockFeatures.map {
            FeatureTimeSeries(_, generateMockTimeSeriesPoints(startTs, endTs))
          }
          GroupByTimeSeries(g, featureTS)
        }

        val mockTSData = JoinTimeSeriesResponse(name, groupByTimeSeries)
        Ok(mockTSData.asJson.noSpaces)
    }
  }

  private def doFetchJoinSkew(name: String,
                              startTs: Long,
                              endTs: Long,
                              metric: Metric,
                              sliceId: Option[String]): Result = {
    val mockGroupBys = generateMockGroupBys(3)
    val groupByTimeSeries = mockGroupBys.map { g =>
      val mockFeatures = generateMockFeatures(g, 10)
      val featureTS = mockFeatures.map {
        FeatureTimeSeries(_, generateMockTimeSeriesPoints(startTs, endTs))
      }
      GroupByTimeSeries(g, featureTS)
    }

    val mockTSData = JoinTimeSeriesResponse(name, groupByTimeSeries)
    val json = mockTSData.asJson.noSpaces
    Ok(json)
  }

  private def doFetchFeature(name: String,
                             startTs: Long,
                             endTs: Long,
                             metricType: String,
                             metrics: String,
                             slice: Option[String],
                             granularity: String,
                             offset: Option[String],
                             algorithm: Option[String]): Action[AnyContent] =
    Action { implicit request: Request[AnyContent] =>
      val metricChoice = parseMetricChoice(Some(metricType))
      val metricRollup = parseMetricRollup(Some(metrics))
      val granularityType = parseGranularity(granularity)

      (metricChoice, metricRollup, granularityType) match {
        case (None, _, _) => BadRequest("Invalid metric choice. Expect drift / skew")
        case (_, None, _) => BadRequest("Invalid metric rollup. Expect null / value")
        case (_, _, None) => BadRequest("Invalid granularity. Expect raw / percentile / aggregates")
        case (Some(Drift), Some(rollup), Some(g)) =>
          doFetchFeatureDrift(name, startTs, endTs, rollup, slice, g, offset, algorithm)
        case (Some(Skew), Some(rollup), Some(g)) => doFetchFeatureSkew(name, startTs, endTs, rollup, slice, g)
      }
    }

  private def doFetchFeatureDrift(name: String,
                                  startTs: Long,
                                  endTs: Long,
                                  metric: Metric,
                                  sliceId: Option[String],
                                  granularity: Granularity,
                                  offset: Option[String],
                                  algorithm: Option[String]): Result = {
    if (granularity == Raw) {
      BadRequest("We don't support Raw granularity for drift metric types")
    } else {
      (parseOffset(offset), parseAlgorithm(algorithm)) match {
        case (None, _)          => BadRequest(s"Unable to parse offset - $offset")
        case (_, None)          => BadRequest("Invalid drift algorithm. Expect PSI or KL")
        case (Some(_), Some(_)) =>
          // TODO: Use parsedOffset and parsedAlgorithm when ready
          val featureTsJson = if (granularity == Aggregates) {
            // if feature name ends in an even digit we consider it continuous and generate mock data accordingly
            // else we generate mock data for a categorical feature
            val featureId = name.split("_").last.toInt
            val featureTs = if (featureId % 2 == 0) {
              ComparedFeatureTimeSeries(name,
                                        generateMockRawTimeSeriesPoints(startTs, 100),
                                        generateMockRawTimeSeriesPoints(startTs, 100))
            } else {
              ComparedFeatureTimeSeries(name,
                                        generateMockCategoricalTimeSeriesPoints(startTs, 5, 1),
                                        generateMockCategoricalTimeSeriesPoints(startTs, 5, 2))
            }
            featureTs.asJson
          } else {
            FeatureTimeSeries(name, generateMockTimeSeriesPercentilePoints(startTs, endTs)).asJson
          }
          Ok(featureTsJson.noSpaces)
      }
    }
  }

  private def doFetchFeatureSkew(name: String,
                                 startTs: Long,
                                 endTs: Long,
                                 metric: Metric,
                                 sliceId: Option[String],
                                 granularity: Granularity): Result = {
    if (granularity == Aggregates) {
      BadRequest("We don't support Aggregates granularity for skew metric types")
    } else {
      val featureTsJson = if (granularity == Raw) {
        val featureTs = ComparedFeatureTimeSeries(name,
                                                  generateMockRawTimeSeriesPoints(startTs, 100),
                                                  generateMockRawTimeSeriesPoints(startTs, 100))
        featureTs.asJson.noSpaces
      } else {
        val featuresTs = FeatureTimeSeries(name, generateMockTimeSeriesPercentilePoints(startTs, endTs))
        featuresTs.asJson.noSpaces
      }
      Ok(featureTsJson)
    }
  }
}

object TimeSeriesController {

  def parseOffset(offset: Option[String]): Option[Duration] = {
    val hourPattern = """(\d+)h""".r
    val dayPattern = """(\d+)d""".r
    offset.map(_.toLowerCase) match {
      case Some(hourPattern(num)) => Some(num.toInt.hours)
      case Some(dayPattern(num))  => Some(num.toInt.days)
      case _                      => None
    }
  }

  def parseAlgorithm(algorithm: Option[String]): Option[DriftAlgorithm] = {
    algorithm.map(_.toLowerCase) match {
      case Some("psi") => Some(PSI)
      case Some("kl")  => Some(KL)
      case _           => None
    }
  }

  def parseMetricChoice(metricType: Option[String]): Option[MetricType] = {
    metricType.map(_.toLowerCase) match {
      case Some("drift") => Some(Drift)
      case Some("skew")  => Some(Skew)
      case Some("ooc")   => Some(Skew)
      case _             => None
    }
  }

  def parseMetricRollup(metrics: Option[String]): Option[Metric] = {
    metrics.map(_.toLowerCase) match {
      case Some("null")  => Some(NullMetric)
      case Some("value") => Some(ValuesMetric)
      case _             => None
    }
  }

  def parseGranularity(granularity: String): Option[Granularity] = {
    granularity.toLowerCase match {
      case "raw"        => Some(Raw)
      case "percentile" => Some(Percentile)
      case "aggregates" => Some(Aggregates)
      case _            => None
    }
  }

  // !!!!! Mock generation code !!!!! //

  val mockGeneratedPercentiles: Seq[String] =
    Seq("p0", "p10", "p20", "p30", "p40", "p50", "p60", "p70", "p75", "p80", "p90", "p95", "p99", "p100")

  // temporarily serve up mock data while we wait on hooking up our KV store layer + drift calculation
  private def generateMockTimeSeriesPoints(startTs: Long, endTs: Long): Seq[TimeSeriesPoint] = {
    val random = new Random(1000)
    (startTs until endTs by (1.hours.toMillis)).map(ts => TimeSeriesPoint(random.nextDouble(), ts))
  }

  private def generateMockRawTimeSeriesPoints(timestamp: Long, count: Int): Seq[TimeSeriesPoint] = {
    val random = new Random(1000)
    (0 until count).map(_ => TimeSeriesPoint(random.nextDouble(), timestamp))
  }

  private def generateMockCategoricalTimeSeriesPoints(timestamp: Long,
                                                      categoryCount: Int,
                                                      nullCategoryCount: Int): Seq[TimeSeriesPoint] = {
    val random = new Random(1000)
    val catTSPoints = (0 until categoryCount).map(i => TimeSeriesPoint(random.nextInt(1000), timestamp, Some(s"A_$i")))
    val nullCatTSPoints = (0 until nullCategoryCount).map(i =>
      TimeSeriesPoint(random.nextDouble(), timestamp, Some(s"A_{$i + $categoryCount}"), Some(random.nextInt(10))))
    catTSPoints ++ nullCatTSPoints
  }

  private def generateMockTimeSeriesPercentilePoints(startTs: Long, endTs: Long): Seq[TimeSeriesPoint] = {
    val random = new Random(1000)
    (startTs until endTs by (1.hours.toMillis)).flatMap { ts =>
      mockGeneratedPercentiles.zipWithIndex.map {
        case (p, _) => TimeSeriesPoint(random.nextDouble(), ts, Some(p))
      }
    }
  }

  private def generateMockGroupBys(numGroupBys: Int): Seq[String] =
    (1 to numGroupBys).map(i => s"my_groupby_$i")

  private def generateMockFeatures(groupBy: String, featuresPerGroupBy: Int): Seq[String] =
    (1 to featuresPerGroupBy).map(i => s"$groupBy.my_feature_$i")

}
