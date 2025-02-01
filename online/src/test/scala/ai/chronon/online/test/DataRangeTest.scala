package ai.chronon.online.test

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.PartitionSpec
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.online.PartitionRange
import ai.chronon.online.PartitionRange.collapseToRange
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataRangeTest extends AnyFlatSpec with Matchers {

  // Assuming you have a PartitionSpec and PartitionRange class defined somewhere
  implicit val partitionSpec: PartitionSpec = new PartitionSpec("yyyy-MM-dd", new Window(1, TimeUnit.DAYS).millis)

  "collapseToRange" should "collapse consecutive partitions into ranges" in {
    val partitions = List(
      "2020-01-01",
      "2020-01-02",
      "2020-01-03",
      "2020-01-05",
      "2020-01-07",
      "2020-01-08"
    )

    val expectedRanges = Seq(
      PartitionRange("2020-01-01", "2020-01-03"),
      PartitionRange("2020-01-05", "2020-01-05"),
      PartitionRange("2020-01-07", "2020-01-08")
    )

    val result = collapseToRange(partitions)
    println(result)
    result should have length 3
    result should contain theSameElementsInOrderAs expectedRanges
  }

  it should "handle a single partition" in {
    val partitions = List("2020-01-01")
    val expectedRanges = Seq(PartitionRange("2020-01-01", "2020-01-01"))

    val result = collapseToRange(partitions)

    result should have length 1
    result should contain theSameElementsAs expectedRanges
  }

  it should "handle empty input" in {
    val partitions = List.empty[String]

    val result = collapseToRange(partitions)

    result should be(empty)
  }

  it should "handle non-consecutive dates" in {
    val partitions = List("2020-01-01", "2020-01-03", "2020-01-05", "2020-01-07")
    val expectedRanges = Seq(
      PartitionRange("2020-01-01", "2020-01-01"),
      PartitionRange("2020-01-03", "2020-01-03"),
      PartitionRange("2020-01-05", "2020-01-05"),
      PartitionRange("2020-01-07", "2020-01-07")
    )

    val result = collapseToRange(partitions)

    result should have length 4
    result should contain theSameElementsInOrderAs expectedRanges
  }
}
