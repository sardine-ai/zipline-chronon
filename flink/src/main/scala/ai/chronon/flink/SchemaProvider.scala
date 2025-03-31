package ai.chronon.flink

import ai.chronon.api
import ai.chronon.api.GroupBy
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, DeserializationSchema}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row

/** A SchemaProvider is responsible for building a DeserializationSchema for a given topic.
  *
  * This class handles looking up the schema and then based on the schema type (e.g. Avro, Protobuf) it will create
  * the appropriate DeserializationSchema. Default implementations of the SchemaProvider return  DeserializationSchemas
  * that pass through all fields in the source event. DeSerializationSchemas that push projection pushdown to the source
  * will mixin the SourceProjection trait.
  * @param conf - Configuration for the SchemaProvider (we pick this up from the topicInfo param map)
  */
abstract class SchemaProvider[T](conf: Map[String, String]) {
  def buildDeserializationSchema(groupBy: GroupBy): ChrononDeserializationSchema[T]
}

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
