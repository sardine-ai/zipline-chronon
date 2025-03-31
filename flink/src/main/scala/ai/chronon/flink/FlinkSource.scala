package ai.chronon.flink

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

abstract class FlinkSource[T] extends Serializable {

  /** Return a Flink DataStream for the given topic and groupBy.
    *
    * When implementing a source, you should also make a conscious decision about your allowed lateness strategy.
    */
  def getDataStream(topic: String, groupByName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int
  ): SingleOutputStreamOperator[T]
}
