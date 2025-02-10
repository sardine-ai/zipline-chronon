package ai.chronon.api

import ai.chronon.api.thrift.TDeserializer
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.protocol.TBinaryProtocol
import ai.chronon.api.thrift.protocol.TProtocolFactory
import ai.chronon.fetcher.TileKey

import java.io.Serializable
import scala.jdk.CollectionConverters._

// Convenience functions for working with tiling
object TilingUtils {
  class SerializableSerializer(factory: TProtocolFactory) extends TSerializer(factory) with Serializable

  // crazy bug in compact protocol - do not change to compact

  @transient
  lazy val binarySerializer: ThreadLocal[TSerializer] = new ThreadLocal[TSerializer] {
    override def initialValue(): TSerializer = new TSerializer(new TBinaryProtocol.Factory())
  }

  @transient
  lazy val binaryDeserializer: ThreadLocal[TDeserializer] = new ThreadLocal[TDeserializer] {
    override def initialValue(): TDeserializer = new TDeserializer(new TBinaryProtocol.Factory())
  }

  def serializeTileKey(key: TileKey): Array[Byte] = {
    binarySerializer.get().serialize(key)
  }

  def deserializeTileKey(bytes: Array[Byte]): TileKey = {
    val key = new TileKey()
    binaryDeserializer.get().deserialize(key, bytes)
    key
  }

  def buildTileKey(dataset: String,
                   keyBytes: Array[Byte],
                   tileSizeMs: Option[Long],
                   tileStartTs: Option[Long]): TileKey = {
    val tileKey = new TileKey()
    tileKey.setDataset(dataset)
    tileKey.setKeyBytes(keyBytes.toList.asJava.asInstanceOf[java.util.List[java.lang.Byte]])
    tileSizeMs.foreach(tileKey.setTileSizeMillis)
    tileStartTs.foreach(tileKey.setTileStartTimestampMillis)
    tileKey
  }
}
