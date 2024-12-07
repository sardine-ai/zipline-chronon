package controllers
import ai.chronon.api.DriftMetric
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.TileDriftSeries
import ai.chronon.api.TileSummarySeries
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.online.stats.DriftStore
import io.circe.generic.auto._
import io.circe.syntax._
import model._
import play.api.mvc._

import javax.inject._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.ScalaJavaConversions.ListOps
import scala.util.ScalaJavaConversions.MapOps
import scala.util.Success

/**
  * Controller that serves various time series endpoints at the model, join and feature level
  */
@Singleton
class TimeSeriesController @Inject() (val controllerComponents: ControllerComponents, driftStore: DriftStore)(implicit
    ec: ExecutionContext)
    extends BaseController {

  import TimeSeriesController._

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
    doFetchJoin(name, startTs, endTs, metricType, metrics, offset, algorithm)

  /**
    * Helps retrieve a time series (drift or skew) for a given feature. Time series is
    * retrieved between the start and end ts. Choice of granularity (raw, aggregate, percentiles) along with the
    * metric type (drift / skew) dictates the shape of the returned time series.
    */
  def fetchFeature(join: String,
                   name: String,
                   startTs: Long,
                   endTs: Long,
                   metricType: String,
                   metrics: String,
                   granularity: String,
                   offset: Option[String],
                   algorithm: Option[String]): Action[AnyContent] =
    doFetchFeature(join, name, startTs, endTs, metricType, metrics, granularity, offset, algorithm)

  private def doFetchJoin(name: String,
                          startTs: Long,
                          endTs: Long,
                          metricType: String,
                          metrics: String,
                          offset: Option[String],
                          algorithm: Option[String]): Action[AnyContent] =
    Action.async { implicit request: Request[AnyContent] =>
      val metricChoice = parseMetricChoice(Some(metricType))
      val metricRollup = parseMetricRollup(Some(metrics))

      (metricChoice, metricRollup) match {
        case (None, _)                   => Future.successful(BadRequest("Invalid metric choice. Expect drift"))
        case (_, None)                   => Future.successful(BadRequest("Invalid metric rollup. Expect null / value"))
        case (Some(Drift), Some(rollup)) => doFetchJoinDrift(name, startTs, endTs, rollup, offset, algorithm)
      }
    }

  private def doFetchJoinDrift(name: String,
                               startTs: Long,
                               endTs: Long,
                               metric: Metric,
                               offset: Option[String],
                               algorithm: Option[String]): Future[Result] = {

    (parseOffset(offset), parseAlgorithm(algorithm)) match {
      case (None, _) => Future.successful(BadRequest(s"Unable to parse offset - $offset"))
      case (_, None) => Future.successful(BadRequest("Invalid drift algorithm. Expect JSD, PSI or Hellinger"))
      case (Some(o), Some(driftMetric)) =>
        val window = new Window(o.toMinutes.toInt, TimeUnit.MINUTES)
        val joinPath = name.replaceFirst("\\.", "/") // we need to look up in the drift store with this transformed name
        val maybeDriftSeries = driftStore.getDriftSeries(joinPath, driftMetric, window, startTs, endTs)
        maybeDriftSeries match {
          case Failure(exception) =>
            Future.successful(InternalServerError(s"Error computing join drift - ${exception.getMessage}"))
          case Success(driftSeriesFuture) =>
            driftSeriesFuture.map { driftSeries =>
              // pull up a list of drift series objects for all the features in a group
              val grpToDriftSeriesList: Map[String, Seq[TileDriftSeries]] = driftSeries.groupBy(_.key.groupName)
              val groupByTimeSeries = grpToDriftSeriesList.map {
                case (name, featureDriftSeriesInfoSeq) =>
                  GroupByTimeSeries(
                    name,
                    featureDriftSeriesInfoSeq.map(series => convertTileDriftSeriesInfoToTimeSeries(series, metric)))
              }.toSeq

              val tsData = JoinTimeSeriesResponse(name, groupByTimeSeries)
              Ok(tsData.asJson.noSpaces)
            }
        }
    }
  }

  private def doFetchFeature(join: String,
                             name: String,
                             startTs: Long,
                             endTs: Long,
                             metricType: String,
                             metrics: String,
                             granularity: String,
                             offset: Option[String],
                             algorithm: Option[String]): Action[AnyContent] =
    Action.async { implicit request: Request[AnyContent] =>
      val metricChoice = parseMetricChoice(Some(metricType))
      val metricRollup = parseMetricRollup(Some(metrics))
      val granularityType = parseGranularity(granularity)

      (metricChoice, metricRollup, granularityType) match {
        case (None, _, _) => Future.successful(BadRequest("Invalid metric choice. Expect drift"))
        case (_, None, _) => Future.successful(BadRequest("Invalid metric rollup. Expect null / value"))
        case (_, _, None) => Future.successful(BadRequest("Invalid granularity. Expect raw / percentile / aggregates"))
        case (Some(Drift), Some(rollup), Some(g)) =>
          doFetchFeatureDrift(join, name, startTs, endTs, rollup, g, offset, algorithm)
      }
    }

  private def doFetchFeatureDrift(join: String,
                                  name: String,
                                  startTs: Long,
                                  endTs: Long,
                                  metric: Metric,
                                  granularity: Granularity,
                                  offset: Option[String],
                                  algorithm: Option[String]): Future[Result] = {
    def checkIfNumeric(summarySeries: TileSummarySeries) = {
      summarySeries.percentiles.asScala != null && summarySeries.percentiles.asScala.exists(_ != null)
    }

    if (granularity == Raw) {
      Future.successful(BadRequest("We don't support Raw granularity for drift metric types"))
    } else {
      (parseOffset(offset), parseAlgorithm(algorithm)) match {
        case (None, _) => Future.successful(BadRequest(s"Unable to parse offset - $offset"))
        case (_, None) => Future.successful(BadRequest("Invalid drift algorithm. Expect JSD, PSI or Hellinger"))
        case (Some(o), Some(driftMetric)) =>
          val window = new Window(o.toMinutes.toInt, TimeUnit.MINUTES)
          val joinPath =
            join.replaceFirst("\\.", "/") // we need to look up in the drift store with this transformed name
          if (granularity == Aggregates) {
            val maybeDriftSeries =
              driftStore.getDriftSeries(joinPath, driftMetric, window, startTs, endTs, Some(name))
            maybeDriftSeries match {
              case Failure(exception) =>
                Future.successful(InternalServerError(s"Error computing feature drift - ${exception.getMessage}"))
              case Success(driftSeriesFuture) =>
                driftSeriesFuture.map { driftSeries =>
                  val featureTs = convertTileDriftSeriesInfoToTimeSeries(driftSeries.head, metric)
                  Ok(featureTs.asJson.noSpaces)
                }
            }
          } else {
            // percentiles
            val maybeCurrentSummarySeries = driftStore.getSummarySeries(joinPath, startTs, endTs, Some(name))
            val maybeBaselineSummarySeries =
              driftStore.getSummarySeries(joinPath, startTs - window.millis, endTs - window.millis, Some(name))
            (maybeCurrentSummarySeries, maybeBaselineSummarySeries) match {
              case (Failure(exceptionA), Failure(exceptionB)) =>
                Future.successful(InternalServerError(
                  s"Error computing feature percentiles for current + offset time window.\nCurrent window error: ${exceptionA.getMessage}\nOffset window error: ${exceptionB.getMessage}"))
              case (_, Failure(exception)) =>
                Future.successful(
                  InternalServerError(
                    s"Error computing feature percentiles for offset time window - ${exception.getMessage}"))
              case (Failure(exception), _) =>
                Future.successful(
                  InternalServerError(
                    s"Error computing feature percentiles for current time window - ${exception.getMessage}"))
              case (Success(currentSummarySeriesFuture), Success(baselineSummarySeriesFuture)) =>
                Future.sequence(Seq(currentSummarySeriesFuture, baselineSummarySeriesFuture)).map { merged =>
                  val currentSummarySeries = merged.head
                  val baselineSummarySeries = merged.last

                  val isCurrentNumeric = currentSummarySeries.headOption.forall(checkIfNumeric)
                  val isBaselineNumeric = baselineSummarySeries.headOption.forall(checkIfNumeric)

                  val currentFeatureTs = {
                    if (currentSummarySeries.isEmpty) Seq.empty
                    else convertTileSummarySeriesToTimeSeries(currentSummarySeries.head, isCurrentNumeric, metric)
                  }
                  val baselineFeatureTs = {
                    if (baselineSummarySeries.isEmpty) Seq.empty
                    else convertTileSummarySeriesToTimeSeries(baselineSummarySeries.head, isBaselineNumeric, metric)
                  }
                  val comparedTsData =
                    ComparedFeatureTimeSeries(name, isCurrentNumeric, baselineFeatureTs, currentFeatureTs)
                  Ok(comparedTsData.asJson.noSpaces)
                }
            }
          }
      }
    }
  }

  private def convertTileDriftSeriesInfoToTimeSeries(tileDriftSeries: TileDriftSeries,
                                                     metric: Metric): FeatureTimeSeries = {
    // check if we have a numeric / categorical feature. If the percentile drift series has non-null doubles
    // then we have a numeric feature at hand
    val isNumeric =
      tileDriftSeries.percentileDriftSeries.asScala != null && tileDriftSeries.percentileDriftSeries.asScala
        .exists(_ != null)
    val lhsList = if (metric == NullMetric) {
      tileDriftSeries.nullRatioChangePercentSeries.asScala
    } else {
      if (isNumeric) tileDriftSeries.percentileDriftSeries.asScala
      else tileDriftSeries.histogramDriftSeries.asScala
    }
    val points = lhsList.zip(tileDriftSeries.timestamps.asScala).map {
      case (v, ts) => TimeSeriesPoint(v, ts)
    }

    FeatureTimeSeries(tileDriftSeries.getKey.getColumn, isNumeric, points)
  }

  private def convertTileSummarySeriesToTimeSeries(summarySeries: TileSummarySeries,
                                                   isNumeric: Boolean,
                                                   metric: Metric): Seq[TimeSeriesPoint] = {
    if (metric == NullMetric) {
      summarySeries.nullCount.asScala.zip(summarySeries.timestamps.asScala).map {
        case (nullCount, ts) => TimeSeriesPoint(0, ts, nullValue = Some(nullCount.intValue()))
      }
    } else {
      if (isNumeric) {
        val percentileSeriesPerBreak = summarySeries.percentiles.toScala
        val timeStamps = summarySeries.timestamps.toScala
        val breaks = DriftStore.breaks(20)
        percentileSeriesPerBreak.zip(breaks).flatMap {
          case (percentileSeries, break) =>
            percentileSeries.toScala.zip(timeStamps).map { case (value, ts) => TimeSeriesPoint(value, ts, Some(break)) }
        }
      } else {
        val histogramOfSeries = summarySeries.histogram.toScala
        val timeStamps = summarySeries.timestamps.toScala
        histogramOfSeries.flatMap {
          case (label, values) =>
            values.toScala.zip(timeStamps).map { case (value, ts) => TimeSeriesPoint(value.toDouble, ts, Some(label)) }
        }.toSeq
      }
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

  def parseAlgorithm(algorithm: Option[String]): Option[DriftMetric] = {
    algorithm.map(_.toLowerCase) match {
      case Some("psi")       => Some(DriftMetric.PSI)
      case Some("hellinger") => Some(DriftMetric.HELLINGER)
      case Some("jsd")       => Some(DriftMetric.JENSEN_SHANNON)
      case _                 => None
    }
  }

  // We currently only support drift
  def parseMetricChoice(metricType: Option[String]): Option[MetricType] = {
    metricType.map(_.toLowerCase) match {
      case Some("drift") => Some(Drift)
//      case Some("skew")  => Some(Skew)
//      case Some("ooc")   => Some(Skew)
      case _ => None
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
}
