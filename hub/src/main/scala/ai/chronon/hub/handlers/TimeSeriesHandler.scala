package ai.chronon.hub.handlers

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.hub.model.Aggregates
import ai.chronon.hub.model.ComparedFeatureTimeSeries
import ai.chronon.hub.model.Drift
import ai.chronon.hub.model.FeatureTimeSeries
import ai.chronon.hub.model.Granularity
import ai.chronon.hub.model.GroupByTimeSeries
import ai.chronon.hub.model.JoinTimeSeriesResponse
import ai.chronon.hub.model.Metric
import ai.chronon.hub.model.MetricType
import ai.chronon.hub.model.NullMetric
import ai.chronon.hub.model.Percentile
import ai.chronon.hub.model.Raw
import ai.chronon.hub.model.TimeSeriesPoint
import ai.chronon.hub.model.ValuesMetric
import ai.chronon.observability.DriftMetric
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.stats.DriftStore
import io.circe.generic.auto._
import io.circe.syntax._
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future => ScalaFuture}
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

/**
  * Controller that serves various time series endpoints at the join and feature level
  */
class TimeSeriesHandler(driftStore: DriftStore) {

  import TimeSeriesHandler._
  import VertxExtensions._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Helps retrieve a time series (drift or skew) for each of the features that are part of a join. Time series is
    * retrieved between the start and end ts. If the metric type is for drift, the offset is used to compute the
    * distribution to compare against (we compare current time range with the same sized time range starting offset time
    * period prior). We compute either the null drift / skew or value's drift or skew based on the
    * metric choice.
    */
  val joinDriftHandler: Handler[RoutingContext] = (ctx: RoutingContext) => {
    // /api/v1/join/:name/timeseries
    val name = ctx.pathParam("name")
    val startTs = getMandatoryQueryParam(ctx, "startTs").toLong
    val endTs = getMandatoryQueryParam(ctx, "endTs").toLong
    val metricType = getMandatoryQueryParam(ctx, "metricType")
    val metrics = getMandatoryQueryParam(ctx, "metrics")
    val offset = Option(ctx.queryParams.get("offset"))
    val algorithm = Option(ctx.queryParams.get("algorithm"))

    val metricChoice = parseMetricChoice(Some(metricType))
    val metricRollup = parseMetricRollup(Some(metrics))

    (metricChoice, metricRollup) match {
      case (None, _)                   => ctx.BadRequest("Invalid metric choice. Expect drift")
      case (_, None)                   => ctx.BadRequest("Invalid metric rollup. Expect null / value")
      case (Some(Drift), Some(rollup)) => doFetchJoinDrift(ctx, name, startTs, endTs, rollup, offset, algorithm)
    }
  }

  private def doFetchJoinDrift(ctx: RoutingContext,
                               name: String,
                               startTs: Long,
                               endTs: Long,
                               metric: Metric,
                               offset: Option[String],
                               algorithm: Option[String]): Unit = {

    (parseOffset(offset), parseAlgorithm(algorithm)) match {
      case (None, _) => ctx.BadRequest(s"Unable to parse offset - $offset")
      case (_, None) => ctx.BadRequest("Invalid drift algorithm. Expect JSD, PSI or Hellinger")
      case (Some(o), Some(driftMetric)) =>
        val window = new Window(o.toMinutes.toInt, TimeUnit.MINUTES)
        val joinPath = name.replaceFirst("\\.", "/") // we need to look up in the drift store with this transformed name
        val maybeDriftSeries = driftStore.getDriftSeries(joinPath, driftMetric, window, startTs, endTs)
        maybeDriftSeries match {
          case Failure(exception) =>
            ctx.InternalServerError(s"Error computing join drift - ${exception.getMessage}")
          case Success(driftSeriesFuture) =>
            val scalaJsonResultFuture = driftSeriesFuture.map { driftSeries =>
              // pull up a list of drift series objects for all the features in a group
              val grpToDriftSeriesList: Map[String, Seq[TileDriftSeries]] = driftSeries.groupBy(_.key.groupName)
              val groupByTimeSeries = grpToDriftSeriesList.map {
                case (name, featureDriftSeriesInfoSeq) =>
                  GroupByTimeSeries(
                    name,
                    featureDriftSeriesInfoSeq.map(series => convertTileDriftSeriesInfoToTimeSeries(series, metric)))
              }.toSeq

              val tsData = JoinTimeSeriesResponse(name, groupByTimeSeries)
              tsData.asJson.noSpaces
            }(driftStore.executionContext)

            transformScalaFutureToVertxResponse(ctx, scalaJsonResultFuture)
        }
    }
  }

  private def convertTileDriftSeriesInfoToTimeSeries(tileDriftSeries: TileDriftSeries,
                                                     metric: Metric): FeatureTimeSeries = {
    // check if we have a numeric / categorical feature. If the percentile drift series has non-null doubles
    // then we have a numeric feature at hand
    val isNumeric =
      Option(tileDriftSeries.percentileDriftSeries).exists(series => series.asScala.exists(_ != null))

    val lhsList = if (metric == NullMetric) {
      Option(tileDriftSeries.nullRatioChangePercentSeries).map(_.asScala).getOrElse(Seq.empty)
    } else {
      if (isNumeric)
        Option(tileDriftSeries.percentileDriftSeries).map(_.asScala).getOrElse(Seq.empty)
      else
        Option(tileDriftSeries.histogramDriftSeries).map(_.asScala).getOrElse(Seq.empty)
    }
    val points = lhsList.zip(tileDriftSeries.timestamps.asScala).map {
      case (v, ts) => TimeSeriesPoint(v, ts)
    }

    FeatureTimeSeries(tileDriftSeries.getKey.getColumn, isNumeric, points)
  }

  /**
    * Helps retrieve a time series (drift or skew) for a given feature. Time series is
    * retrieved between the start and end ts. Choice of granularity (raw, aggregate, percentiles) along with the
    * metric type (drift / skew) dictates the shape of the returned time series.
    */
  val featureDriftHandler: Handler[RoutingContext] = (ctx: RoutingContext) => {
    // /api/v1/join/:join/feature/:name/timeseries

    val join = ctx.pathParam("join")
    val name = ctx.pathParam("name")
    val startTs = getMandatoryQueryParam(ctx, "startTs").toLong
    val endTs = getMandatoryQueryParam(ctx, "endTs").toLong
    val metricType = getMandatoryQueryParam(ctx, "metricType")
    val metrics = getMandatoryQueryParam(ctx, "metrics")
    val granularity = getMandatoryQueryParam(ctx, "granularity")
    val offset = Option(ctx.queryParams.get("offset"))
    val algorithm = Option(ctx.queryParams.get("algorithm"))

    val metricChoice = parseMetricChoice(Some(metricType))
    val metricRollup = parseMetricRollup(Some(metrics))
    val granularityType = parseGranularity(granularity)

    (metricChoice, metricRollup, granularityType) match {
      case (None, _, _) => ctx.BadRequest("Invalid metric choice. Expect drift")
      case (_, None, _) => ctx.BadRequest("Invalid metric rollup. Expect null / value")
      case (_, _, None) => ctx.BadRequest("Invalid granularity. Expect raw / percentile / aggregates")
      case (Some(Drift), Some(rollup), Some(g)) =>
        doFetchFeatureDrift(ctx, join, name, startTs, endTs, rollup, g, offset, algorithm)
    }
  }

  private def doFetchFeatureDrift(ctx: RoutingContext,
                                  join: String,
                                  name: String,
                                  startTs: Long,
                                  endTs: Long,
                                  metric: Metric,
                                  granularity: Granularity,
                                  offset: Option[String],
                                  algorithm: Option[String]): Unit = {
    if (granularity == Raw) {
      ctx.BadRequest("We don't support Raw granularity for drift metric types")
    } else {
      (parseOffset(offset), parseAlgorithm(algorithm)) match {
        case (None, _) => ctx.BadRequest(s"Unable to parse offset - $offset")
        case (_, None) => ctx.BadRequest("Invalid drift algorithm. Expect JSD, PSI or Hellinger")
        case (Some(o), Some(driftMetric)) =>
          val window = new Window(o.toMinutes.toInt, TimeUnit.MINUTES)
          val joinPath =
            join.replaceFirst("\\.", "/") // we need to look up in the drift store with this transformed name
          if (granularity == Aggregates) {
            doFetchFeatureDriftAggregates(ctx, name, startTs, endTs, metric, driftMetric, window, joinPath)
          } else {
            doFetchFeaturePercentileDrift(ctx, name, startTs, endTs, metric, window, joinPath)
          }
      }
    }
  }

  private def doFetchFeaturePercentileDrift(ctx: RoutingContext,
                                            name: String,
                                            startTs: Long,
                                            endTs: Long,
                                            metric: Metric,
                                            window: Window,
                                            joinPath: String): Unit = {
    def checkIfNumeric(summarySeries: TileSummarySeries) = {
      summarySeries.percentiles.asScala != null && summarySeries.percentiles.asScala.exists(_ != null)
    }

    // percentiles
    val maybeCurrentSummarySeries = driftStore.getSummarySeries(joinPath, startTs, endTs, Some(name))
    val maybeBaselineSummarySeries =
      driftStore.getSummarySeries(joinPath, startTs - window.millis, endTs - window.millis, Some(name))
    (maybeCurrentSummarySeries, maybeBaselineSummarySeries) match {
      case (Failure(exceptionA), Failure(exceptionB)) =>
        ctx.InternalServerError(
          s"Error computing feature percentiles for current + offset time window.\nCurrent window error: ${exceptionA.getMessage}\nOffset window error: ${exceptionB.getMessage}")
      case (_, Failure(exception)) =>
        ctx.InternalServerError(s"Error computing feature percentiles for offset time window - ${exception.getMessage}")
      case (Failure(exception), _) =>
        ctx.InternalServerError(
          s"Error computing feature percentiles for current time window - ${exception.getMessage}")
      case (Success(currentSummarySeriesFuture), Success(baselineSummarySeriesFuture)) =>
        implicit val ec: ExecutionContext = driftStore.executionContext
        val scalaJsonFuture = {
          ScalaFuture.sequence(Seq(currentSummarySeriesFuture, baselineSummarySeriesFuture)).map { merged =>
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
            comparedTsData.asJson.noSpaces
          }
        }

        transformScalaFutureToVertxResponse(ctx, scalaJsonFuture)
    }
  }

  private def doFetchFeatureDriftAggregates(ctx: RoutingContext,
                                            name: String,
                                            startTs: Long,
                                            endTs: Long,
                                            metric: Metric,
                                            driftMetric: DriftMetric,
                                            window: Window,
                                            joinPath: String): Unit = {
    val maybeDriftSeries =
      driftStore.getDriftSeries(joinPath, driftMetric, window, startTs, endTs, Some(name))

    maybeDriftSeries match {
      case Failure(exception) =>
        ctx.InternalServerError(s"Error computing feature drift - ${exception.getMessage}")
      case Success(driftSeriesFuture) =>
        val scalaJsonResultFuture = driftSeriesFuture.map { driftSeries =>
          val featureTs = convertTileDriftSeriesInfoToTimeSeries(driftSeries.head, metric)
          featureTs.asJson.noSpaces
        }(driftStore.executionContext)
        transformScalaFutureToVertxResponse(ctx, scalaJsonResultFuture)
    }
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

  def transformScalaFutureToVertxResponse(ctx: RoutingContext, scalaJsonResultFuture: ScalaFuture[String]): Unit = {
    val vertxResultFuture =
      Future.fromCompletionStage(FutureConverters.toJava(scalaJsonResultFuture).toCompletableFuture)
    vertxResultFuture.onSuccess { tsData =>
      ctx.Ok(tsData)
    }
    vertxResultFuture.onFailure { err =>
      ctx.InternalServerError(s"Error fetching join drift: ${err.getMessage}")
    }
  }

}

object TimeSeriesHandler {

  def getMandatoryQueryParam(ctx: RoutingContext, name: String): String = {
    Option(ctx.queryParams.get(name)).getOrElse(throw new IllegalArgumentException(s"Missing $name"))
  }

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
