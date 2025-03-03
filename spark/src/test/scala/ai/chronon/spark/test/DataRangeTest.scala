/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import ai.chronon.api.PartitionSpec
import ai.chronon.api.PartitionRange
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class DataRangeTest extends AnyFlatSpec {
  val spark: SparkSession = SparkSessionBuilder.build("DataRangeTest", local = true)
  private implicit val partitionSpec: PartitionSpec = TableUtils(spark).partitionSpec

  it should "intersect" in {
    val range1 = PartitionRange(null, null)
    val range2 = PartitionRange("2023-01-01", "2023-01-02")
    assertEquals(range2, range1.intersect(range2))
  }
}
