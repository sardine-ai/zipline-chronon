package ai.chronon.hub.model

/** Captures some details related to ML models registered with Zipline to surface in the Hub UI */
case class GroupBy(name: String, features: Seq[String])
case class Join(name: String,
                joinFeatures: Seq[String],
                groupBys: Seq[GroupBy],
                online: Boolean,
                production: Boolean,
                team: Option[String])
case class Model(name: String, join: Join, online: Boolean, production: Boolean, team: String, modelType: String)

/** Supported Metric types */
sealed trait MetricType

/** Drift between two distributions */
case object Drift extends MetricType

/** Training / serving or online/offline consistency */
case object Skew extends MetricType

/** Supported drift algorithms */
sealed trait DriftAlgorithm

/** Population Stability Index */
case object PSI extends DriftAlgorithm

/** Kullback-Leibler (KL) divergence */
case object KL extends DriftAlgorithm

/** Supported metric rollups */
sealed trait Metric

/** Roll up over null counts */
case object NullMetric extends Metric

/** Roll up over raw values */
case object ValuesMetric extends Metric

/** Supported granularity types */
sealed trait Granularity

/** Raw - return raw distribution values */
case object Raw extends Granularity

/** Percentile - return percentile distribution values */
case object Percentile extends Granularity

/** Aggregates - compute aggregated metrics (e.g. drift) */
case object Aggregates extends Granularity

case class TimeSeriesPoint(value: Double, ts: Long, label: Option[String] = None, nullValue: Option[Int] = None)
case class FeatureTimeSeries(feature: String, isNumeric: Boolean, points: Seq[TimeSeriesPoint])
case class ComparedFeatureTimeSeries(feature: String,
                                     isNumeric: Boolean,
                                     baseline: Seq[TimeSeriesPoint],
                                     current: Seq[TimeSeriesPoint])
case class GroupByTimeSeries(name: String, items: Seq[FeatureTimeSeries])

// Currently search only covers joins
case class ListModelResponse(offset: Int, items: Seq[Model])
case class SearchJoinResponse(offset: Int, items: Seq[Join])
case class ListJoinResponse(offset: Int, items: Seq[Join])

case class ModelTimeSeriesResponse(id: String, items: Seq[TimeSeriesPoint])
case class JoinTimeSeriesResponse(name: String, items: Seq[GroupByTimeSeries])
