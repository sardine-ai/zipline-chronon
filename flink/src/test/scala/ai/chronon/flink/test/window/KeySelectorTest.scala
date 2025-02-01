package ai.chronon.flink.test.window

import ai.chronon.api.Builders
import ai.chronon.flink.window.KeySelectorBuilder
import org.scalatest.flatspec.AnyFlatSpec

class KeySelectorTest extends AnyFlatSpec {
  it should "chronon flink job correctly keys by a groupbys entity keys" in {
    // We expect something like this to come out of the SparkExprEval operator
    val sampleSparkExprEvalOutput: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")

    val groupByWithOneEntityKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelectorBuilder.build(groupByWithOneEntityKey)
    assert(
      keyFunctionOne.getKey(sampleSparkExprEvalOutput) == List(4242)
    )

    val groupByWithTwoEntityKey = Builders.GroupBy(keyColumns = Seq("number", "user"))
    val keyFunctionTwo = KeySelectorBuilder.build(groupByWithTwoEntityKey)
    assert(
      keyFunctionTwo.getKey(sampleSparkExprEvalOutput) == List(4242, "abc")
    )
  }

  it should "key selector function returns same hashes for lists with the same content" in {
    // This is more of a sanity check. It's not comprehensive.
    // SINGLE ENTITY KEY
    val map1: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val map2: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "10.0.0.1", "user" -> "notabc")
    val groupBySingleKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelectorBuilder.build(groupBySingleKey)
    assert(
      keyFunctionOne.getKey(map1).hashCode() == keyFunctionOne.getKey(map2).hashCode()
    )

    // TWO ENTITY KEYS
    val map3: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val map4: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> 4242, "user" -> "notabc")
    val groupByTwoKeys = Builders.GroupBy(keyColumns = Seq("number", "ip"))
    val keyFunctionTwo = KeySelectorBuilder.build(groupByTwoKeys)
    assert(
      keyFunctionTwo.getKey(map3).hashCode() == keyFunctionTwo.getKey(map4).hashCode()
    )

    val map5: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    val map6: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    assert(keyFunctionTwo.getKey(map5).hashCode() == keyFunctionTwo.getKey(map6).hashCode())
  }
}
