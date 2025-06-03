package ai.chronon.online.test

import ai.chronon.online.LoggableResponse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class LoggableResponseSerDeSpec extends AnyFlatSpec {

  it should "correct handle loggable response round trip" in {
    val keyBytes = "testKey".getBytes("UTF-8")
    val valueBytes = "testValue".getBytes("UTF-8")
    val joinName = "test.join"
    val schemaHash = "abcd"
    val loggableResponse = LoggableResponse(keyBytes, valueBytes, joinName, 123L, schemaHash)

    val avroBytes = LoggableResponse.toAvroBytes(loggableResponse)
    avroBytes should not be null

    val deserializedResponse = LoggableResponse.fromAvroBytes(avroBytes)
    deserializedResponse should not be null
    deserializedResponse.keyBytes shouldEqual keyBytes
    deserializedResponse.valueBytes shouldEqual valueBytes
    deserializedResponse.joinName shouldEqual joinName
    deserializedResponse.tsMillis shouldEqual 123L
    deserializedResponse.schemaHash shouldEqual schemaHash
  }
}
