package ai.chronon.online.connectors

import ai.chronon.api.DataSpec
import org.apache.flink.streaming.api.scala.{createTypeInformation, DataStream => FlinkStream}
import org.apache.flink.api.common.typeinfo.TypeInformation

trait Serde {
  def toBytes[T](t: T): Array[Byte]
  def fromBytes[T](bytes: Array[Byte]): T
}

abstract class MessageBus(catalog: Catalog) {
  protected def createTopicInternal(topic: Topic, spec: DataSpec): Unit
  protected def writeBytes(topic: Topic, data: Array[Byte], key: Array[Byte] = null): Unit
  protected def buildFlinkStream(topic: Topic): FlinkStream[Array[Byte]]

  // helpers
  def createTopic(topic: Topic, spec: DataSpec): Unit = {
    createTopicInternal(topic, spec)
    catalog.putSpec(topic, spec)
  }

  def write[T](topic: Topic, t: T, ser: Serde): Unit = {
    writeBytes(topic, ser.toBytes(t))
  }
  def buildFlinkStream[T: TypeInformation](topic: Topic, ser: Serde): FlinkStream[T] =
    buildFlinkStream(topic).map(b => ser.fromBytes(b).asInstanceOf[T])
}
