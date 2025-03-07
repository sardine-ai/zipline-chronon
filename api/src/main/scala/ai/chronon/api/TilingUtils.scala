package ai.chronon.api

import ai.chronon.fetcher.TileKey

import scala.jdk.CollectionConverters._

// Convenience functions for working with tiling
object TilingUtils {

  def serializeTileKey(key: TileKey): Array[Byte] = {
    SerdeUtils.compactSerializer.get().serialize(key)
  }

  def deserializeTileKey(bytes: Array[Byte]): TileKey = {
    val key = new TileKey()
    SerdeUtils.compactDeserializer.get().deserialize(key, bytes)
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
