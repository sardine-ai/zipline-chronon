package ai.chronon.flink

import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row

/** A SchemaProvider is responsible for providing the Encoder and DeserializationSchema for a given topic.
  * This class handles looking up the schema and then based on the schema type (e.g. Avro, Protobuf) it will create
  * the appropriate Encoder and DeserializationSchema. The Encoder is needed for SparkExpressionEval and the DeserializationSchema
  * is needed to allow Flink's Kafka / other sources to crack open the Array[Byte] payloads.
  * @param conf - Configuration for the SchemaProvider (we pick this up from the topicInfo param map)
  */
abstract class SchemaProvider(conf: Map[String, String]) {
  def buildEncoderAndDeserSchema(topicInfo: TopicInfo): (Encoder[Row], DeserializationSchema[Row])
}
