package ai.chronon.spark.stats.drift

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions._
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import ai.chronon.spark.udafs.ArrayApproxDistinct
import ai.chronon.spark.udafs.ArrayStringHistogramAggregator
import ai.chronon.spark.udafs.HistogramAggregator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udaf

import scala.util.Try

import Expressions.{CardinalityExpression, High, Low, SummaryExpression}

// join / sq / gb: metadata.monitors{} -> drift & skew
// TODO override for metric
class Summarizer(name: String,
                 df: DataFrame,
                 timeColumn: Option[String] = None,
                 sliceColumns: Option[Seq[String]] = None,
                 derivedColumns: Option[Map[String, String]] = None,
                 includeColumns: Option[Seq[String]] = None,
                 excludeColumns: Option[Seq[String]] = None,
                 tileSize: Window = new Window(5, TimeUnit.MINUTES),
                 cardinalityThreshold: Int = 100)(implicit val tu: TableUtils) {

  // prune down to the set of columns to summarize + validations
  private val (cardinalityInputDf, summaryInputDf): (DataFrame, DataFrame) = {

    println(s"Original schema:\n${df.schema}".green)

    val derivedDf = derivedColumns
      .map { dc =>
        val derivedColumns = dc.map { case (k, v) => s"$v as `$k`" }.toSeq
        // original columns that are not colliding with derived map
        // all valid slice columns will be included into the result
        val originalColumns = df.columns.filterNot(dc.keySet.contains)
        val derivedDf = df.selectExpr(originalColumns ++ derivedColumns: _*)
        println(s"Schema after derivations:\n${derivedDf.schema}".green)
        derivedDf
      }
      .getOrElse(df)

    if (includeColumns.nonEmpty) {
      val unknownColumns = includeColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown columns to include: ${unknownColumns.mkString(", ")}")
    }

    if (excludeColumns.nonEmpty) {
      val unknownColumns = excludeColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown columns to exclude: ${unknownColumns.mkString(", ")}")
    }

    if (sliceColumns.nonEmpty) {
      val unknownColumns = sliceColumns.get.filterNot(derivedDf.columns.contains)
      assert(unknownColumns.isEmpty, s"Unknown slice columns: ${unknownColumns.mkString(", ")}")
    }

    val derivedDfWithTile = injectTile(derivedDf)

    val summaryColumns = (includeColumns
      .getOrElse(derivedDfWithTile.columns.toSeq)
      .filterNot(excludeColumns.getOrElse(Seq.empty).contains) ++
      sliceColumns.getOrElse(Seq.empty)).distinct

    val cardinalityColumns = summaryColumns
      .filterNot((tu.partitionColumn +: timeColumn.toSeq +: Constants.TileColumn).contains)

    assert(cardinalityColumns.nonEmpty, "No columns selected for cardinality estimation")
    assert(summaryColumns.nonEmpty, "No columns selected for summarization")

    println(s"Derived df with tile: ${derivedDfWithTile.columns.mkString(", ")}")
    val cardinalityInputDf = derivedDfWithTile.select(cardinalityColumns.head, cardinalityColumns.tail: _*)
    println(s"Schema of columns to estimate cardinality for:\n${cardinalityInputDf.schema}".green)

    val summaryInputDf = derivedDfWithTile.select(summaryColumns.head, summaryColumns.tail: _*)
    println(s"Schema of columns to summarize:\n${summaryInputDf.schema}".green)

    cardinalityInputDf -> summaryInputDf
  }

  // inject a tile column into the data frame to compute summaries within.
  private def injectTile(derivedDf: DataFrame): DataFrame = {
    val derivedCols = derivedDf.columns
    assert(!derivedCols.contains(Constants.TileColumn),
           s"Time column ${Constants.TileColumn} is reserved. Please use a different name.")

    // adds a time based tile column for a given time expression in milliseconds
    def addTileCol(ts: String): DataFrame = {
      val tileExpr = s"round(($ts)/${tileSize.millis}) * ${tileSize.millis}"
      println(derivedDf.columns.mkString(", ").yellow)
      println("tile_expr: ".yellow + tileExpr)
      val result = derivedDf.withColumn(Constants.TileColumn, expr(tileExpr))

      if (derivedCols.contains(ts)) result.drop(ts) else result
    }

    val timeCol = timeColumn.getOrElse(
      if (derivedCols.contains(Constants.TimeColumn)) Constants.TimeColumn
      else null
    )

    if (timeCol != null) {
      addTileCol(timeCol)
    } else if (derivedCols.contains(tu.partitionColumn)) {
      val conversionEx = s"unix_timestamp(to_timestamp(${tu.partitionColumn}, '${tu.partitionSpec.format}')) * 1000"
      // no rounding if no millisTime - just tile by partition column
      println("Ignoring tileSize since no time column specified, tiling by the partition instead".blue)
      derivedDf.withColumn(Constants.TileColumn, expr(conversionEx)).drop(tu.partitionColumn)
    } else {
      // nothing to groupBy using a dummy
      derivedDf.withColumn(Constants.TileColumn, lit(0.0))
    }
  }

  // TODO - persist this after first computation into kvstore
  private lazy val cardinalityMap: Map[String, Long] = {

    val exprs: Seq[(String, String)] = cardinalityInputDf.schema.fields
      .flatMap { f =>
        val ceTry = Try {
          CardinalityExpression(f.dataType).render(f.name)
        }
        if (ceTry.isFailure) {
          println(s"Cannot compute cardinality of column ${f.name}: ${ceTry.failed.get.getMessage}".red)
          None
        } else {
          val ce = ceTry.get
          Some(ce.select.get -> ce.aggExpr)
        }
      }

    println(s"Extracting countable structures using expressions:\n  ${exprs.map(_._1).mkString("\n  ")}")
    val inputTransformed = cardinalityInputDf.selectExpr(exprs.map(_._1): _*)
    inputTransformed.limit(5).show()
    println(s"Aggregating countable structures using expressions:\n  ${exprs.map(_._2).mkString("\n  ")}")
    val spark = df.sparkSession
    // register udaf
    spark.udf.register("array_dbl_distinct", udaf(new ArrayApproxDistinct[Double]()))
    val aggregated = inputTransformed.selectExpr(exprs.map(_._2): _*)
    val counts = aggregated.collect().head.getValuesMap[Long](aggregated.columns)
    println(s"Counts for each field:\n  ${counts.mkString(",\n  ")}")

    // verify that all slices are low cardinality
    for (
      cols <- sliceColumns;
      col <- cols;
      count <- counts.get(col)
    ) {
      assert(count <= cardinalityThreshold, s"Slice column $col is high cardinality $count")
    }
    counts
  }

  def computeSummaryDf: DataFrame = {
    val summaryExpressions = summaryInputDf.schema.fields.flatMap { f =>
      val cardinality = if (cardinalityMap.contains(f.name)) {
        if (cardinalityMap(f.name) <= cardinalityThreshold) Low else High
      } else {
        println(s"Cardinality not computed for column ${f.name}".yellow)
        Low
      }
      SummaryExpression.of(f.dataType, cardinality).map(_.render(f.name))
    }

    summaryInputDf.createTempView("summary_input")

    val keys = Constants.TileColumn + (if (summaryInputDf.columns.contains(tu.partitionColumn))
                                         s", ${tu.partitionColumn}"
                                       else "")
    val mapQuery =
      s"""SELECT
         |  ${summaryExpressions.flatMap(_.mapEx).mkString(",\n  ")},
         |  $keys
         |FROM summary_input""".stripMargin

    val mappedDf = tu.sql(mapQuery)
    println(s"Schema of mapped columns:\n${mappedDf.schema}".green)
    mappedDf.createTempView("mapped_input")

    df.sparkSession.udf.register("count_histogram", udaf(HistogramAggregator))
    df.sparkSession.udf.register("array_histogram", udaf(ArrayStringHistogramAggregator))

    val aggQuery =
      s"""SELECT
         |  $keys,
         |  ${summaryExpressions.map(_.aggEx).mkString(",\n  ")}
         |FROM mapped_input
         |GROUP BY $keys""".stripMargin

    val aggDf = tu.sql(aggQuery)
    println(s"Schema of aggregated columns:\n${aggDf.schema}".green)

    if (aggDf.schema.contains(tu.partitionColumn)) {
      aggDf.save(s"${name}_drift")
    } else {
      aggDf.saveUnPartitioned(s"${name}_drift")
    }

    aggDf
  }
}
