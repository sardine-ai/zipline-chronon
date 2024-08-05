package ai.chronon.online

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream => FlinkStream}

// TODO deprecate this in favor of Api.readTopic + Api.streamDecoder
abstract class FlinkSource[T] extends Serializable {

  /**
    * Return a Flink DataStream for the given topic and groupBy.
    *
    * When implementing a source, you should also make a conscious decision about your allowed lateness strategy.
    */
  def getDataStream(topic: String, groupByName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int
  ): FlinkStream[T]
}
