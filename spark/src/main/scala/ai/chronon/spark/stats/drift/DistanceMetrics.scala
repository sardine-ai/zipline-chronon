package ai.chronon.spark.stats

import ai.chronon.spark.Extensions.ArrayOps

import scala.math._

object DriftMetrics {
  // Helper function to calculate probabilities from percentiles
  def percentilesToProbs(percentiles: Seq[Double]): Seq[Double] = {
    val diffs = percentiles.sliding(2).map(pair => pair(1) - pair(0)).toSeq
    val total = diffs.sum
    diffs.map(_ / total)
  }

  def jensenShannonDivergence(p: Seq[Double], q: Seq[Double]): Double = {
    def klDivergence(p: Seq[Double], q: Seq[Double]): Double =
      p.zip(q).filter { case (pi, qi) => pi > 0 && qi > 0 }.map { case (pi, qi) => pi * log(pi / qi) }.sum

    val pProbs = percentilesToProbs(p)
    val qProbs = percentilesToProbs(q)
    val mProbs = pProbs.zip(qProbs).map { case (pi, qi) => (pi + qi) / 2 }

    (klDivergence(pProbs, mProbs) + klDivergence(qProbs, mProbs)) / 2
  }

  def hellingerDistance(p: Seq[Double], q: Seq[Double]): Double = {
    val pProbs = percentilesToProbs(p)
    val qProbs = percentilesToProbs(q)

    sqrt(
      pProbs
        .zip(qProbs)
        .map {
          case (pi, qi) =>
            pow(sqrt(pi) - sqrt(qi), 2)
        }
        .sum / 2)
  }

  def kolmogorovSmirnovDistance(p: Seq[Double], q: Seq[Double]): Double = {
    p.zip(q).map { case (pi, qi) => abs(pi - qi) }.max / 200.0 // Divide by range (200) to normalize
  }

  def populationStabilityIndex(p: Seq[Double], q: Seq[Double]): Double = {
    val pProbs = percentilesToProbs(p)
    val qProbs = percentilesToProbs(q)

    pProbs
      .zip(qProbs)
      .map {
        case (pi, qi) =>
          if (pi > 0 && qi > 0) (qi - pi) * log(qi / pi)
          else 0.0 // Handle zero probabilities
      }
      .sum
  }

  def main(args: Array[String]): Unit = {
    val p =
      Seq(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200).map(_.toDouble)
    val q =
      Seq(5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115, 125, 135, 145, 155, 165, 175, 185, 195, 205).map(_.toDouble)

    println(s"Jensen-Shannon Divergence: ${jensenShannonDivergence(p, q)}")
    println(s"Hellinger Distance: ${hellingerDistance(p, q)}")
    println(s"Kolmogorov-Smirnov Distance: ${kolmogorovSmirnovDistance(p, q)}")
    println(s"Population Stability Index: ${populationStabilityIndex(p, q)}")
  }
}

object JensenShannonDivergenceCalculator {

  def main(args: Array[String]): Unit = {
    val A = Array(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200).map(
      _.toDouble)
    val B = Array(5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115, 125, 135, 145, 155, 165, 175, 185, 195, 205).map(
      _.toDouble)

    val jsd = computeJSD(A, B)
    println(f"The Jensen-Shannon Divergence between distributions A and B is: $jsd%.5f")
  }

  def computeJSD(A: Array[Double], B: Array[Double]): Double = {
    // Step 1: Define combined breakpoints
    val breakpoints = (A ++ B).distinct.sorted

    // Step 2: Compute PDFs for each interval
    val pdfA = computePDF(A, breakpoints)
    val pdfB = computePDF(B, breakpoints)

    // Step 3: Compute the mixture distribution M
    val pdfM = pdfA.zip(pdfB).map { case (a, b) => 0.5 * (a + b) }

    // Step 4: Compute the Jensen-Shannon Divergence
    val klAM = klDivergence(pdfA, pdfM)
    val klBM = klDivergence(pdfB, pdfM)

    0.5 * (klAM + klBM)
  }

  def computePdfs(a: Array[Double], b: Array[Double]): Array[(Double, Double)] = {
    require(a.length == b.length, "Arrays must have the same length")
    val breaks = (a ++ b).uniqSort(Ordering.Double)
    val pdfA = computePDF(a, breaks)
    val pdfB = computePDF(b, breaks)
    pdfA.zip(pdfB)
  }

  def computePDF(percentiles: Array[Double], breaks: Array[Double]): Array[Double] = {
    val interval: Double = 1.toDouble / (percentiles.length - 1)
    var i = 0
    breaks.map { break =>
      var equalityHits = 0
      while (i < percentiles.length && percentiles(i) <= break) {
        if (percentiles(i) == break) equalityHits += 1
        i += 1
      }
      // after this point percentiles(i) > break & percentiles(i-1) <= break

      if (equalityHits == 1) { // not a point-mass
        if (i == percentiles.length) {
          // reached end - look back and assign
          assert(percentiles.length > 1, "need more than one element to compute pdf")
          interval / (percentiles(i - 1) - percentiles(i - 2))
        } else {
          interval / (percentiles(i) - percentiles(i - 1))
        }
      } else if (equalityHits > 1) { // point-mass case
        // point-mass case: when two or more continuous percentile points, pct(i), pct(i+1), pct(i+2)
        // have the same value as the break
        (equalityHits - 1) * interval
      } else { // no equality hits
        interval / (percentiles(i) - percentiles(i - 1))
      }
    }
  }

  def klDivergence(p: Array[Double], q: Array[Double]): Double = {
    p.zip(q).foldLeft(0.0) {
      case (sum, (pi, qi)) =>
        if (pi > 0 && qi > 0) sum + pi * math.log(pi / qi)
        else sum
    }
  }
}
