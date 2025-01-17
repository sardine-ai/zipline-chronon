package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.SparkSessionBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files

class GCSFormatTest extends AnyFlatSpec {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "BigQuerySparkTest",
    local = true
  )

  it should "partitions method should return correctly parsed partitions as maps" in {

    val testData = List(
      ("20241223", "b", "c"),
      ("20241224", "e", "f"),
      ("20241225", "h", "i")
    )

    val dir = Files.createTempDirectory("spark-test-output").toFile
    dir.deleteOnExit()

    val df = spark.createDataFrame(testData).toDF("ds", "first", "second")
    df.write.partitionBy("ds").format("parquet").mode(SaveMode.Overwrite).save(dir.getAbsolutePath)
    val gcsFormat = GCS(sourceUri = dir.getAbsolutePath, fileFormat = "parquet")
    val partitions = gcsFormat.partitions("unused_table")(spark)

    assertEquals(Set(Map("ds" -> "20241223"), Map("ds" -> "20241224"), Map("ds" -> "20241225")), partitions.toSet)

  }

  it should "partitions method should handle empty partitions gracefully" in {

    val testData = List(
      ("20241223", "b", "c"),
      ("20241224", "e", "f"),
      ("20241225", "h", "i")
    )

    val dir = Files.createTempDirectory("spark-test-output").toFile
    dir.deleteOnExit()

    val df = spark.createDataFrame(testData).toDF("ds", "first", "second")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(dir.getAbsolutePath)
    val gcsFormat = GCS(sourceUri = dir.getAbsolutePath, fileFormat = "parquet")
    val partitions = gcsFormat.partitions("unused_table")(spark)

    assertEquals(Set.empty, partitions.toSet)

  }

  it should "partitions method should handle date types" in {
    val testData = List(
      Row("2024-12-23", "b", "c"),
      Row("2024-12-24", "e", "f"),
      Row("2024-12-25", "h", "i")
    )

    val dir = Files.createTempDirectory("spark-test-output").toFile
    dir.deleteOnExit()

    val schema = StructType(
      Seq(
        StructField("ds", StringType, nullable = true),
        StructField("first", StringType, nullable = true),
        StructField("second", StringType, nullable = true)
      ))

    val df =
      spark
        .createDataFrame(spark.sparkContext.parallelize(testData), schema)
        .toDF("ds", "first", "second")
        .select(to_date(col("ds"), "yyyy-MM-dd").as("ds"), col("first"), col("second"))
    df.write.format("parquet").partitionBy("ds").mode(SaveMode.Overwrite).save(dir.getAbsolutePath)
    val gcsFormat = GCS(sourceUri = dir.getAbsolutePath, fileFormat = "parquet")
    val partitions = gcsFormat.partitions("unused_table")(spark)

    assertEquals(Set(Map("ds" -> "2024-12-23"), Map("ds" -> "2024-12-24"), Map("ds" -> "2024-12-25")), partitions.toSet)

  }
}
