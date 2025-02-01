package ai.chronon.orchestration.test

import ai.chronon.orchestration.utils.SequenceMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceMapSpec extends AnyFlatSpec with Matchers {

  "SequenceMap" should "insert and retrieve values with correct indices" in {
    val sequenceMap = new SequenceMap[String, Int]

    // Insert values for a single key
    sequenceMap.insert("key1", 100) should be(0)
    sequenceMap.insert("key1", 200) should be(1)
    sequenceMap.insert("key1", 300) should be(2)

    // Verify retrieval
    sequenceMap.get("key1", 0) should be(100)
    sequenceMap.get("key1", 1) should be(200)
    sequenceMap.get("key1", 2) should be(300)
  }

  it should "maintain unique indices for duplicate values" in {
    val sequenceMap = new SequenceMap[String, Int]

    // Insert same value multiple times
    val firstIndex = sequenceMap.insert("key1", 100)
    val secondIndex = sequenceMap.insert("key1", 100)

    // Should return same index for duplicate values
    firstIndex should be(secondIndex)
    firstIndex should be(0)

    // Verify retrieval
    sequenceMap.get("key1", 0) should be(100)
  }

  it should "handle multiple keys independently" in {
    val sequenceMap = new SequenceMap[String, String]

    // Insert values for different keys
    sequenceMap.insert("key1", "value1") should be(0)
    sequenceMap.insert("key2", "value2") should be(0)
    sequenceMap.insert("key1", "value3") should be(1)

    // Verify independent sequences
    sequenceMap.get("key1", 0) should be("value1")
    sequenceMap.get("key1", 1) should be("value3")
    sequenceMap.get("key2", 0) should be("value2")
  }

  it should "correctly check contains for existing and non-existing values" in {
    val sequenceMap = new SequenceMap[Int, String]

    sequenceMap.insert(1, "test")

    // Check existing combinations
    sequenceMap.contains(1, "test") should be(true)

    // Check non-existing combinations
    sequenceMap.contains(1, "nonexistent") should be(false)
    sequenceMap.contains(2, "test") should be(false)
  }

  it should "throw IllegalArgumentException for null keys" in {
    val sequenceMap = new SequenceMap[String, String]

    val exception = intercept[IllegalArgumentException] {
      sequenceMap.insert(null, "value")
    }
    exception.getMessage should be("requirement failed: Key cannot be null")
  }

  it should "throw IllegalArgumentException for null values" in {
    val sequenceMap = new SequenceMap[String, String]

    val exception = intercept[IllegalArgumentException] {
      sequenceMap.insert("key", null)
    }
    exception.getMessage should be("requirement failed: Value cannot be null")
  }

  it should "throw IllegalArgumentException for negative indices" in {
    val sequenceMap = new SequenceMap[String, String]
    sequenceMap.insert("key", "value")

    val exception = intercept[IllegalArgumentException] {
      sequenceMap.get("key", -1)
    }
    exception.getMessage should be("requirement failed: Index cannot be negative")
  }

  it should "throw IllegalArgumentException for non-existing keys in get" in {
    val sequenceMap = new SequenceMap[String, String]

    val exception = intercept[IllegalArgumentException] {
      sequenceMap.get("nonexistent", 0)
    }
    exception.getMessage should be("requirement failed: Key nonexistent not found")
  }

  it should "throw IllegalArgumentException for non-existing indices" in {
    val sequenceMap = new SequenceMap[String, String]
    sequenceMap.insert("key", "value")

    val exception = intercept[IllegalArgumentException] {
      sequenceMap.get("key", 1)
    }
    exception.getMessage should be("requirement failed: Index 1 not found")
  }

  it should "maintain correct sequence for mixed operations" in {
    val sequenceMap = new SequenceMap[String, Int]

    // Insert some values
    sequenceMap.insert("key", 100) should be(0)
    sequenceMap.insert("key", 200) should be(1)

    // Check contains
    sequenceMap.contains("key", 100) should be(true)
    sequenceMap.contains("key", 200) should be(true)

    // Insert duplicate
    sequenceMap.insert("key", 100) should be(0)

    // Insert new value
    sequenceMap.insert("key", 300) should be(2)

    // Verify final sequence
    sequenceMap.get("key", 0) should be(100)
    sequenceMap.get("key", 1) should be(200)
    sequenceMap.get("key", 2) should be(300)
  }
}
