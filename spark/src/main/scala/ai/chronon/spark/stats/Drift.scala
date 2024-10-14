package ai.chronon.spark.stats

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Constants
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Join
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types

import scala.util.Try

object DriftUtils {

  def isScalar(dataType: types.DataType): Boolean =
    dataType match {
      case types.StringType | types.ShortType | types.BooleanType | types.IntegerType | types.LongType |
          types.FloatType | types.DoubleType =>
        true
      case _ => false
    }

  sealed trait Cardinality

  case object Low extends Cardinality

  case object High extends Cardinality

  /* ---------------------- NOTATION ----------------- */
  // one of those cases where we prioritize readability over idiomatic scala.
  // code would be harder to read without short-form & abbreviated notation

  private object Agg {
    // aggregation expressions - we will substitute out the "col" downstream
    val hist = "count_histogram(_col_)"
    val ptile = "approx_percentile(_col_)"
    val arrPtile = "array_percentile(_col_)"
    val arrHist = "array_histogram(_col_)"
    val arrNulls = "sum(aggregate(_col_, 0, (acc, v) -> if(v is NULL, acc + 1, acc)))/sum(size(_col_))"

    val uniq = "approx_count_distinct(_col_)"
    val arrDblUniq = "array_dbl_distinct(_col_)"
    val arrStrUniq = "array_str_distinct(_col_)"
  }

  // expressions over raw columns
  private object Inp {
    val len = "length(_col_)"
    val cLen = "size(_col_)"
    val strCast = "CAST(_col_ as STRING)"
    val dblCast = "CAST(_col_ as DOUBLE)"
    val arrStrCast = "transform(_col_, x -> CAST(x as STRING))"
    val arrDblCast = "transform(_col_, x -> CAST(x as Double))"
    val mapVals = "map_values(_col_)"
    val lenVals = "CAST(transform(map_values(_col_), x -> len(x)) AS FLOAT)"
    val mapStrCast = "transform(map_values(_col_), x -> CAST(x as STRING))"
    val mapDblCast = "transform(map_values(_col_), x -> CAST(x as DOUBLE))"
  }

  // metric names, "_drift" will be appended to these names downstream
  private object Name {
    val cov = "coverage"
    val dist = "distribution"
    val vCov = "value_coverage"
    val length = "length"
    val vLength = "value_length"
  }

  case class CardinalityExpression(select: Option[String], aggExpr: String)

  object CardinalityExpression {
    private def ce(select: String, aggExpr: String): CardinalityExpression =
      CardinalityExpression(Option(select), aggExpr)

    def apply(dataType: types.DataType): CardinalityExpression = {
      dataType match {
        case dType if isScalar(dType) => ce(null, Agg.uniq)
        case types.ArrayType(elemType, _) =>
          elemType match { // histogram
            case types.StringType         => ce(null, Agg.arrStrUniq)
            case types.DoubleType         => ce(null, Agg.arrDblUniq)
            case eType if isScalar(eType) => ce(Inp.dblCast, Agg.arrDblUniq)
            case _                        => throw new UnsupportedOperationException(s"Unsupported array element type $elemType")
          }
        // TODO: measure and handle map key cardinality
        case types.MapType(_, vType, _) =>
          vType match {
            case types.StringType         => ce(Inp.mapVals, Agg.arrStrUniq)
            case types.DoubleType         => ce(Inp.mapVals, Agg.arrDblUniq)
            case eType if isScalar(eType) => ce(Inp.mapDblCast, Agg.arrDblUniq)
            case _                        => throw new UnsupportedOperationException(s"Unsupported map value type $vType")
          }
        case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
      }
    }
  }

  case class SummaryExpression(mapEx: Option[String], aggEx: String, name: String) {
    def render(col: String): SummaryExpression = {
      val aggregatedColumnName = s"${col}_${name}"
      val inputColName = s"${aggregatedColumnName}_input"
      SummaryExpression(
        Some(s"${mapEx.map(_.replace("_col_", col)).getOrElse(col)} as `$inputColName`"),
        aggEx.replace("_col_", aggregatedColumnName),
        name
      )
    }
  }

  object SummaryExpression {
    private def se(mapEx: String, aggFunc: String, name: String): Seq[SummaryExpression] =
      Seq(SummaryExpression(Option(mapEx), aggFunc, name))

    def of(dataType: types.DataType, cardinality: Cardinality): Seq[SummaryExpression] =
      cardinality match {
        case Low =>
          dataType match {
            case types.StringType         => se(null, Agg.hist, Name.dist)
            case dType if isScalar(dType) => se(Inp.strCast, Agg.hist, Name.dist)
            case types.ArrayType(elemType, _) =>
              se(Inp.cLen, Agg.ptile, Name.length) ++
                se(null, Agg.arrNulls, Name.vCov) ++
                (elemType match { // histogram
                  case types.StringType         => se(null, Agg.arrHist, Name.dist)
                  case eType if isScalar(eType) => se(Inp.arrStrCast, Agg.arrHist, Name.dist)
                  case _                        => Seq.empty
                })
            // TODO: measure and handle map key cardinality
            case types.MapType(_, vType, _) =>
              se(Inp.cLen, Agg.ptile, Name.length) ++ // length drift
                se(Inp.mapVals, Agg.arrNulls, Name.vCov) ++
                (vType match { // histogram of values
                  case types.StringType         => se(Inp.mapVals, Agg.arrHist, Name.dist)
                  case eType if isScalar(eType) => se(Inp.mapStrCast, Agg.arrHist, Name.dist)
                  case _                        => Seq.empty
                })
            case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
          }
        case High =>
          dataType match {
            case types.StringType         => se(Inp.len, Agg.ptile, Name.dist)
            case dType if isScalar(dType) => se(Inp.dblCast, Agg.ptile, Name.dist)
            case types.ArrayType(elemType, _) =>
              se(Inp.cLen, Agg.ptile, Name.length) ++ (elemType match {
                case types.StringType         => se(Inp.len, Agg.ptile, Name.vLength)
                case eType if isScalar(eType) => se(Inp.arrDblCast, Agg.arrPtile, Name.dist)
                case _                        => Seq.empty
              })
            case types.MapType(_, vType, _) =>
              se(Inp.cLen, Agg.ptile, Name.length) ++ (vType match {
                case types.StringType         => se(Inp.lenVals, Agg.arrPtile, Name.vLength)
                case eType if isScalar(eType) => se(Inp.mapStrCast, Agg.arrPtile, Name.dist)
                case _                        => Seq.empty
              })
            case _ => throw new UnsupportedOperationException(s"Unsupported data type $dataType")
          }
      }
  }

  class DataFrameSummary(name: String,
                         df: DataFrame,
                         timeColumn: Option[String] = None,
                         sliceColumns: Option[Seq[String]] = None,
                         derivedColumns: Option[Map[String, String]] = None,
                         includeColumns: Option[Seq[String]] = None,
                         excludeColumns: Option[Seq[String]] = None,
                         tileSize: Window = new Window(5, TimeUnit.MINUTES),
                         cardinalityThreshold: Int = 10000)(implicit val tu: TableUtils) {

    // prune down to the set of columns to summarize + validations
    val (cardinalityInputDf, summaryInputDf): (DataFrame, DataFrame) = {

      println(s"Original schema:\n${df.schema}".green)

      val derivedDf = derivedColumns
        .map { dc =>
          val derivedColumns = dc.map { case (k, v) => s"$v as `$k`" }.toSeq
          // original columns that are not colliding with derived map
          // all valid slice columns will be included into the result
          val originalColumns = df.schema.fieldNames.filterNot(dc.keySet.contains)
          val derivedDf = df.selectExpr(originalColumns ++ derivedColumns: _*)
          println(s"Schema after derivations:\n${derivedDf.schema}".green)
          derivedDf
        }
        .getOrElse(df)

      if (includeColumns.nonEmpty) {
        val unknownColumns = includeColumns.get.filterNot(derivedDf.schema.fieldNames.contains)
        assert(unknownColumns.isEmpty, s"Unknown columns to include: ${unknownColumns.mkString(", ")}")
      }

      if (excludeColumns.nonEmpty) {
        val unknownColumns = excludeColumns.get.filterNot(derivedDf.schema.fieldNames.contains)
        assert(unknownColumns.isEmpty, s"Unknown columns to exclude: ${unknownColumns.mkString(", ")}")
      }

      if (sliceColumns.nonEmpty) {
        val unknownColumns = sliceColumns.get.filterNot(derivedDf.schema.fieldNames.contains)
        assert(unknownColumns.isEmpty, s"Unknown slice columns: ${unknownColumns.mkString(", ")}")
      }

      val derivedCols = derivedDf.schema.fieldNames.toSeq
      val derivedDfWithTile = injectTile(derivedDf)

      val summaryColumns = (includeColumns
        .getOrElse(derivedCols)
        .filterNot(excludeColumns.getOrElse(Seq.empty).contains) ++
        sliceColumns.getOrElse(Seq.empty) :+
        "tile").distinct

      val cardinalityColumns = summaryColumns
        .filterNot((tu.partitionColumn +: timeColumn.toSeq).contains)

      assert(cardinalityColumns.nonEmpty, "No columns selected for cardinality estimation")
      assert(summaryColumns.nonEmpty, "No columns selected for summarization")

      val cardinalityInputDf = derivedDfWithTile.select(cardinalityColumns.head, cardinalityColumns.tail: _*)
      println(s"Schema of columns to estimate cardinality for:\n${cardinalityInputDf.schema}".green)

      val summaryInputDf = derivedDfWithTile.select(summaryColumns.head, summaryColumns.tail: _*)
      println(s"Schema of columns to summarize:\n${summaryInputDf.schema}".green)

      cardinalityInputDf -> summaryInputDf
    }

    private val tileColumn: String = "_tile"
    // inject a tile column into the data frame to compute summaries within.
    private def injectTile(derivedDf: DataFrame): DataFrame = {
      val derivedCols = derivedDf.schema.fieldNames
      assert(!derivedCols.contains(tileColumn), s"Time column $tileColumn is reserved. Please use a different name.")

      // adds a time based tile column for a given time expression in milliseconds
      def addTileCol(ts: String): DataFrame = {
        val tileExpr = s"round((${ts})/${tileSize.millis}) * ${tileSize.millis})"
        val result = derivedDf.withColumn(tileColumn, expr(tileExpr))
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
        derivedDf.withColumn(tileColumn, expr(conversionEx)).drop(tu.partitionColumn)
      } else {
        // nothing to groupBy using a dummy
        derivedDf.withColumn(tileColumn, lit(0).cast("long"))
      }
    }

    // TODO - persist this after first computation into kvstore
    lazy val cardinalityMap: Map[String, Long] = {

      val exprs: Seq[(String, String)] = cardinalityInputDf.schema.fields
        .flatMap { f =>
          val ceTry = Try {
            CardinalityExpression(f.dataType)
          }
          if (ceTry.isFailure) {
            println(s"Cannot compute cardinality of column ${f.name}: ${ceTry.failed.get.getMessage}".red)
            None
          } else {
            val ce = ceTry.get
            val select = ce.select.map(s => s"$s as `${f.name}`").getOrElse(f.name)
            val agg = ce.aggExpr.replace("_col_", f.name)
            Some(select -> agg)
          }
        }

      println(s"Extracting countable structures using expressions:\n  ${exprs.map(_._1).mkString("\n  ")}")
      val inputTransformed = df.selectExpr(exprs.map(_._1): _*)
      inputTransformed.limit(5).show()
      println(s"Aggregating countable structures using expressions:\n  ${exprs.map(_._2).mkString("\n  ")}")
      val aggregated = inputTransformed.selectExpr(exprs.map(_._2): _*)
      val counts = aggregated.collect().head.getValuesMap[Long](aggregated.schema.fieldNames)
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

    def summaryDf: DataFrame = {
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

      val mapQuery =
        s"""SELECT
           |  ${summaryExpressions.flatMap(_.mapEx).mkString(",\n  ")},
           |  $tileColumn
           |FROM summary_input""".stripMargin

      val mappedDf = tu.sql(mapQuery)
      println(s"Schema of mapped columns:\n${mappedDf.schema}".green)
      mappedDf.createTempView("mapped_input")

      val keys = s"$tileColumn" + (if (summaryInputDf.schema.fieldNames.contains(tu.partitionColumn))
                                     s", $tu.partitionColumn"
                                   else "")

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

  object DataFrameSummary {
    def apply(join: Join): DataFrameSummary = {
      join.metaData.outputTable
      join.metaData.driftTable
      null
//      val timeColumn = None
//      val sliceColumns = join.sliceColumns
//      val derivedColumns = join.derivedColumns
//      val includeColumns = join.includeColumns
//      val excludeColumns = join.excludeColumns
//      val tileSize = join.tileSize
//      val cardinalityThreshold = join.cardinalityThreshold
//      implicit val tu = join.tu
//      new DataFrameSummary(join.name, inputDf, timeColumn, sliceColumns, derivedColumns, includeColumns, excludeColumns, tileSize, cardinalityThreshold)
    }
  }
}
