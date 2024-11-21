package ai.chronon.online.test.stats

import ai.chronon.api.DriftMetric
import ai.chronon.online.stats.DriftMetrics.histogramDistance
import ai.chronon.online.stats.DriftMetrics.percentileDistance
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.ScalaJavaConversions.JMapOps

class DriftMetricsTest extends AnyFunSuite with Matchers {

  def buildPercentiles(mean: Double, variance: Double, breaks: Int = 20): Array[Double] = {
    val stdDev = math.sqrt(variance)

    val probPoints = (0 to breaks).map { i =>
      if (i == 0) 0.01
      else if (i == breaks) 0.99
      else i.toDouble / breaks
    }.toArray

    probPoints.map { p =>
      val standardNormalPercentile = math.sqrt(2) * inverseErf(2 * p - 1)
      mean + (stdDev * standardNormalPercentile)
    }
  }

  def inverseErf(x: Double): Double = {
    val a = 0.147
    val signX = if (x >= 0) 1 else -1
    val absX = math.abs(x)

    val term1 = math.pow(2 / (math.Pi * a) + math.log(1 - absX * absX) / 2, 0.5)
    val term2 = math.log(1 - absX * absX) / a

    signX * math.sqrt(term1 - term2)
  }
  type Histogram = java.util.Map[String, java.lang.Long]

  def compareDistributions(meanA: Double,
                           varianceA: Double,
                           meanB: Double,
                           varianceB: Double,
                           breaks: Int = 20,
                           debug: Boolean = false): Map[DriftMetric, (Double, Double)] = {

    val aPercentiles = buildPercentiles(meanA, varianceA, breaks)
    val bPercentiles = buildPercentiles(meanB, varianceB, breaks)

    val aHistogram: Histogram = (0 to breaks)
      .map { i =>
        val value = java.lang.Long.valueOf((math.abs(aPercentiles(i)) * 100).toLong)
        i.toString -> value
      }
      .toMap
      .toJava

    val bHistogram: Histogram = (0 to breaks)
      .map { i =>
        val value = java.lang.Long.valueOf((math.abs(bPercentiles(i)) * 100).toLong)
        i.toString -> value
      }
      .toMap
      .toJava

    def calculateDrift(metric: DriftMetric): (Double, Double) = {
      val pDrift = percentileDistance(aPercentiles, bPercentiles, metric, debug = debug)
      val histoDrift = histogramDistance(aHistogram, bHistogram, metric)
      (pDrift, histoDrift)
    }

    Map(
      DriftMetric.JENSEN_SHANNON -> calculateDrift(DriftMetric.JENSEN_SHANNON),
      DriftMetric.PSI -> calculateDrift(DriftMetric.PSI),
      DriftMetric.HELLINGER -> calculateDrift(DriftMetric.HELLINGER)
    )
  }

  test("Low drift - similar distributions") {
    val drifts = compareDistributions(meanA = 100.0, varianceA = 225.0, meanB = 101.0, varianceB = 225.0)

    // JSD assertions
    val (jsdPercentile, jsdHistogram) = drifts(DriftMetric.JENSEN_SHANNON)
    jsdPercentile should be < 0.05
    jsdHistogram should be < 0.05

    // Hellinger assertions
    val (hellingerPercentile, hellingerHisto) = drifts(DriftMetric.HELLINGER)
    hellingerPercentile should be < 0.05
    hellingerHisto should be < 0.05
  }

  test("Moderate drift - slightly different distributions") {
    val drifts = compareDistributions(meanA = 100.0, varianceA = 225.0, meanB = 105.0, varianceB = 256.0)

    // JSD assertions
    val (jsdPercentile, _) = drifts(DriftMetric.JENSEN_SHANNON)
    jsdPercentile should (be >= 0.05 and be <= 0.15)

    // Hellinger assertions
    val (hellingerPercentile, _) = drifts(DriftMetric.HELLINGER)
    hellingerPercentile should (be >= 0.05 and be <= 0.15)
  }

  test("Severe drift - different means") {
    val drifts = compareDistributions(meanA = 100.0, varianceA = 225.0, meanB = 110.0, varianceB = 225.0)

    // JSD assertions
    val (jsdPercentile, _) = drifts(DriftMetric.JENSEN_SHANNON)
    jsdPercentile should be > 0.15

    // Hellinger assertions
    val (hellingerPercentile, _) = drifts(DriftMetric.HELLINGER)
    hellingerPercentile should be > 0.15
  }

  test("Severe drift - different variances") {
    val drifts = compareDistributions(meanA = 100.0, varianceA = 225.0, meanB = 105.0, varianceB = 100.0)

    // JSD assertions
    val (jsdPercentile, _) = drifts(DriftMetric.JENSEN_SHANNON)
    jsdPercentile should be > 0.15

    // Hellinger assertions
    val (hellingerPercentile, _) = drifts(DriftMetric.HELLINGER)
    hellingerPercentile should be > 0.15
  }
}
