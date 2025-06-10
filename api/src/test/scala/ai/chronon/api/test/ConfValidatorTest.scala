package ai.chronon.api.test

import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.api.ThriftJsonCodec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.ConfValidator

class ConfValidatorTest extends AnyFlatSpec with Matchers {

  it should "correctly generated test configs will pass the engine validation" in {
    // TODO: unify config location for testing between python and scala
    val jObj = ThriftJsonCodec.fromJsonFile("", check = false)
    ConfValidator.validate("joins", jObj)
  }

}
