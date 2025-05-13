package ai.chronon.flink.deser

import ai.chronon.api
import ai.chronon.api.GroupBy
import ai.chronon.online.serde.SerDe
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.spark.sql.{Encoder, Row}

/** DeserializationSchema for use within Chronon. Includes details such as the source event encoder and if projection is
  * enabled, the projected schema. This is used to both build the Flink sources as well as in the downstream processing
  * operators (e.g. SparkExprEval).
  *
  * @tparam T - Type of the object returned after deserialization. Can be event type (no projection)
  *             or Map[String, Any] (with projection)
  */
abstract class ChrononDeserializationSchema[T] extends AbstractDeserializationSchema[T] {
  def sourceProjectionEnabled: Boolean

  def sourceEventEncoder: Encoder[Row]
}

/** Trait that is mixed in with DeserializationSchemas that support projection pushdown. This trait provides the projected
  * schema that the source event will be projected to.
  */
trait SourceProjection {
  def projectedSchema: Array[(String, api.DataType)]
}

object DeserializationSchemaBuilder {
  def buildSourceIdentityDeserSchema(provider: SerDe, groupBy: GroupBy): ChrononDeserializationSchema[Row] = {
    new SourceIdentityDeserializationSchema(provider, groupBy)
  }

  def buildSourceProjectionDeserSchema(provider: SerDe,
                                       groupBy: GroupBy): ChrononDeserializationSchema[Map[String, Any]] = {
    new SourceProjectionDeserializationSchema(provider, groupBy)
  }
}
