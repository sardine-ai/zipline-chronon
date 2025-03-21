package ai.chronon.flink

import ai.chronon.api
import ai.chronon.api.GroupBy
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

abstract class ChrononDeserializationSchema extends AbstractDeserializationSchema[Map[String, Any]] {
  def projectedSchema: Array[(String, api.DataType)]
}

/** A SchemaProvider is responsible for providing the Encoder and DeserializationSchema for a given topic.
  * This class handles looking up the schema and then based on the schema type (e.g. Avro, Protobuf) it will create
  * the appropriate Encoder and DeserializationSchema. The Encoder is needed for SparkExpressionEval and the DeserializationSchema
  * is needed to allow Flink's Kafka / other sources to crack open the Array[Byte] payloads.
  * @param conf - Configuration for the SchemaProvider (we pick this up from the topicInfo param map)
  */
abstract class ProjectedSchemaProvider(conf: Map[String, String]) {
  def buildProjectedSourceDeserializer(groupBy: GroupBy): ChrononDeserializationSchema
}
