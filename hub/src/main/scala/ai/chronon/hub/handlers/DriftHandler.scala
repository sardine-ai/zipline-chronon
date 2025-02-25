package ai.chronon.hub.handlers

import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.observability.JoinDriftRequest
import ai.chronon.observability.JoinDriftResponse
import ai.chronon.observability.JoinSummaryRequest
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.stats.DriftStore
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.collection.Seq

class DriftHandler(driftStore: DriftStore) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def getDriftSeriesWithWindow(req: JoinDriftRequest): Seq[TileDriftSeries] = {
    logger.debug(s"Processing drift request for join: ${req.getName}, algorithm: ${req.getAlgorithm}, feature: ${Option(
      req.getColumnName).getOrElse("none")}")

    val offsetDuration = parseOffset(Option(req.getOffset)) match {
      case Some(duration) =>
        logger.debug(s"Parsed offset duration: $duration")
        duration
      case None =>
        logger.error(s"Failed to parse offset: ${req.getOffset}")
        throw new IllegalArgumentException(s"Unable to parse offset - ${req.getOffset}")
    }

    val window = new Window(offsetDuration.toMinutes.toInt, TimeUnit.MINUTES)
    logger.debug(s"Querying drift store with window: $window, name: $req.getName")

    driftStore.getDriftSeries(
      req.getName,
      req.getAlgorithm,
      window,
      req.getStartTs,
      req.getEndTs,
      Option(req.getColumnName)
    ) match {
      case Success(driftSeriesFuture) =>
        val result = Await.result(driftSeriesFuture, 30.seconds)
        logger.debug(s"Successfully retrieved ${result.size} drift series entries")
        result
      case Failure(exception) =>
        logger.error("Failed to retrieve drift series", exception)
        throw new RuntimeException(s"Error getting drift - ${exception.getMessage}")
    }
  }

  def getJoinDrift(req: JoinDriftRequest): JoinDriftResponse = {
    val driftSeries = getDriftSeriesWithWindow(req)
    new JoinDriftResponse().setDriftSeries(driftSeries.asJava)
  }

  def getColumnDrift(req: JoinDriftRequest): TileDriftSeries = {
    val driftSeries = getDriftSeriesWithWindow(req)
    driftSeries.headOption.getOrElse(new TileDriftSeries())
  }

  private def parseOffset(offset: Option[String]): Option[Duration] = {
    logger.debug(s"Parsing offset: $offset")
    val hourPattern = """(\d+)h""".r
    val dayPattern = """(\d+)d""".r
    val result = offset.map(_.toLowerCase) match {
      case Some(hourPattern(num)) => Some(num.toInt.hours)
      case Some(dayPattern(num))  => Some(num.toInt.days)
      case _                      => None
    }
    logger.debug(s"Parsed offset result: $result")
    result
  }

  def getColumnSummary(req: JoinSummaryRequest): TileSummarySeries = {
    logger.debug(s"Processing summary request for join: ${req.getName}, column: ${req.getColumnName}")

    logger.debug(s"Querying summary store with name: $req.getName")

    driftStore.getSummarySeries(
      req.getName,
      req.getStartTs,
      req.getEndTs,
      Some(req.getColumnName)
    ) match {
      case Success(summarySeriesFuture) =>
        val result = Await.result(summarySeriesFuture, 30.seconds)
        logger.debug(s"Successfully retrieved ${result.size} summary series entries")
        result.headOption.getOrElse(new TileSummarySeries())
      case Failure(exception) =>
        logger.error("Failed to retrieve summary series", exception)
        throw new RuntimeException(s"Error getting summary - ${exception.getMessage}")
    }
  }
}
