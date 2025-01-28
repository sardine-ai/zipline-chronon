package ai.chronon.api.test

import ai.chronon.api.Constants
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.observability.TileDriftSeries
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.{Double => JDouble}

class TileSeriesSerializationTest extends AnyFlatSpec with Matchers {

  "TileDriftSeries" should "serialize with nulls and special values" in {
    val tileDriftSeries = new TileDriftSeries()

    val percentileDrifts: Seq[JDouble] = Seq(0.1, null, Double.PositiveInfinity, Double.NaN, 0.5)
      .map(v => 
        if (v == null || (v != null && (v.asInstanceOf[Double].isInfinite || v.asInstanceOf[Double].isNaN)))
          Constants.magicNullDouble
        else
          v.asInstanceOf[JDouble]
      )

    val percentileDriftsList: java.util.List[JDouble] = percentileDrifts.toJava
    tileDriftSeries.setPercentileDriftSeries(percentileDriftsList)

    val jsonStr = ThriftJsonCodec.toJsonStr(tileDriftSeries)

    jsonStr should be ("""{"percentileDriftSeries":[0.1,-2.7980863399423856E16,-2.7980863399423856E16,-2.7980863399423856E16,0.5]}""")
  }


}
