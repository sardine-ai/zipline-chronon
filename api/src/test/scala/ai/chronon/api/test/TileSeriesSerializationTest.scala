package ai.chronon.api.test

import ai.chronon.api.Constants
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.observability.TileDriftSeries
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.{Double => JDouble}

class TileSeriesSerializationTest extends AnyFlatSpec with Matchers {

  "TileDriftSeries" should "serialize with nulls" in {
    val tileDriftSeries = new TileDriftSeries()

    val percentileDrifts: Seq[JDouble] = Seq(0.1, null, 0.3, null, 0.5, null, 0.7, null, 0.9)
      .map(v =>
        if (v == null)
          Constants.magicNullDouble
        else
          v.asInstanceOf[JDouble]
      )

    val percentileDriftsList: java.util.List[JDouble] = percentileDrifts.toJava
    tileDriftSeries.setPercentileDriftSeries(percentileDriftsList)

    val jsonStr = ThriftJsonCodec.toJsonStr(tileDriftSeries)

    jsonStr should be ("""{"percentileDriftSeries":[0.1,-2.7980863399423856E16,0.3,-2.7980863399423856E16,0.5,-2.7980863399423856E16,0.7,-2.7980863399423856E16,0.9]}""")
  }


}
