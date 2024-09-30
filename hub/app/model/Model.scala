package model

/** Captures some details related to ML models registered with Zipline to surface in the Hub UI */
case class Model(name: String,
                 id: String,
                 online: Boolean,
                 production: Boolean,
                 team: String,
                 modelType: String,
                 createTime: Long,
                 lastUpdated: Long)

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

case class TimeSeriesPoint(value: Double, ts: Long, label: Option[String] = None)
case class FeatureTimeSeries(feature: String, points: Seq[TimeSeriesPoint])
case class RawComparedFeatureTimeSeries(feature: String, baseline: Seq[TimeSeriesPoint], current: Seq[TimeSeriesPoint])
case class GroupByTimeSeries(name: String, items: Seq[FeatureTimeSeries])

// Currently search only covers models
case class SearchModelResponse(offset: Int, items: Seq[Model])
case class ListModelResponse(offset: Int, items: Seq[Model])

case class ModelTimeSeriesResponse(id: String, items: Seq[TimeSeriesPoint])
case class JoinTimeSeriesResponse(name: String, items: Seq[GroupByTimeSeries])
