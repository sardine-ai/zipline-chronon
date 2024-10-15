package ai.chronon.spark.stats.drift

import org.apache.spark.sql.types

object Expressions {

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
  // I think, code would be harder to read without short-form & abbreviated notation

  private object Agg {
    private val percentileBreaks: String = (0 to 20).map(_ * 0.05).map(num => f"$num%.2f").toArray.mkString(", ")
    // aggregation expressions - we will substitute out the "col" downstream
    val hist = "count_histogram(_col_)"
    val ptile: String = s"approx_percentile(_col_, array($percentileBreaks))"
    val arrPtile = "array_percentile(_col_)"
    val arrHist = "array_histogram(_col_)"
    val arrNulls = "sum(aggregate(_col_, 0, (acc, v) -> if(v is NULL, acc + 1, acc)))/sum(size(_col_))"

    val uniq = "approx_count_distinct(_col_)"
    val arrDblUniq = "array_dbl_distinct(_col_)"
    val arrStrUniq = "array_str_distinct(_col_)"
  }

  // array or map

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

  case class CardinalityExpression(select: Option[String], aggExpr: String) {
    def render(col: String): CardinalityExpression = {
      val aggregatedColumnName = s"${col}_cardinality"
      val inputColName = s"${aggregatedColumnName}_input"
      CardinalityExpression(
        Some(s"${select.map(_.replace("_col_", col)).getOrElse(col)} as `$inputColName`"),
        s"${aggExpr.replace("_col_", inputColName)} as `$aggregatedColumnName`"
      )
    }
  }

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
            case eType if isScalar(eType) => ce(Inp.arrDblCast, Agg.arrDblUniq)
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

    // expressions are parametric with _col_ within - we replace that with the actual column name
    def render(col: String): SummaryExpression = {
      val aggregatedColumnName = s"${col}_${name}"
      val inputColName = s"${aggregatedColumnName}_input"
      SummaryExpression(
        Some(s"${mapEx.map(_.replace("_col_", col)).getOrElse(col)} as `$inputColName`"),
        s"${aggEx.replace("_col_", inputColName)} as `$aggregatedColumnName`",
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

            // TODO: deal with map keys - as histogram - high cardinality keys vs low cardinality?
            // TODO: heavy hitters - top_k via approx_histogram
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

        // map[string::column, map[string::metric_name, value]]
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
}
