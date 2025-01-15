package ai.chronon.flink

import ai.chronon.online.GroupByServingInfoParsed
import org.apache.spark.sql.Encoder

/**
  * SourceProvider is an abstract class that provides a way to build a source for a Flink job.
  * It takes the groupByServingInfo as an argument and based on the configured GB details, configures
  * the Flink source (e.g. Kafka or PubSub) with the right parallelism etc.
  */
abstract class SourceProvider[T](maybeGroupByServingInfoParsed: Option[GroupByServingInfoParsed]) {
  // Returns a tuple of the source, parallelism
  def buildSource(): (FlinkSource[T], Int)
}

/**
  * EncoderProvider is an abstract class that provides a way to build an Spark encoder for a Flink job.
  * These encoders are used in the SparkExprEval Flink function to convert the incoming stream into types
  * that are amenable for tiled / untiled processing.
  */
abstract class EncoderProvider[T] {
  def buildEncoder(): Encoder[T]
}
