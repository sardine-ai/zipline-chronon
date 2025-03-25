package ai.chronon.api.test

import ai.chronon.api.TilingUtils
import ai.chronon.fetcher.TileKey
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class TilingUtilSpec extends AnyFlatSpec with Matchers {

  "TilingUtils" should "serialize and deserialize TileKey" in {
    val key = new TileKey()
    key.setDataset("MY_GROUPBY_V1_STREAMING")
    key.setKeyBytes("key".getBytes.toList.asJava.asInstanceOf[java.util.List[java.lang.Byte]])
    key.setTileSizeMillis(10.hours.toMillis)
    key.setTileStartTimestampMillis(1738195200000L) // 2025-01-29T00:00:00Z
    val bytes = TilingUtils.serializeTileKey(key)
    val deserializedKey = TilingUtils.deserializeTileKey(bytes)
    deserializedKey.getDataset should be("MY_GROUPBY_V1_STREAMING")
    deserializedKey.getKeyBytes.asScala.map(_.toByte).toArray should be("key".getBytes)
    deserializedKey.getTileSizeMillis should be(10.hours.toMillis)
    deserializedKey.getTileStartTimestampMillis should be(1738195200000L)
  }

  "TilingUtils" should "deser Flink written tile key" in {
    val base64String = "GChTRUFSQ0hfQkVBQ09OU19MSVNUSU5HX0FDVElPTlNfU1RSRUFNSU5HGWMCkJHL6Q0WgLq3AxaAyJ/uuWUA" //  "Aiw=" // "ArL4u9gL"
    val bytes = java.util.Base64.getDecoder.decode(base64String)
    val deserializedKey = TilingUtils.deserializeTileKey(bytes)
    deserializedKey should not be null
  }
}
