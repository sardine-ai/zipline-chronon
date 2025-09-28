package ai.chronon.spark.utils

import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files

/**
 * Abstract base class for tests that require a Spark session with Iceberg catalog configuration.
 * Sets up a Spark session with:
 * - Iceberg Spark extensions
 * - Hadoop tables catalog
 * - Temporary warehouse directory
 * - Local master with all available cores
 */
abstract class SparkTestBase extends AnyFlatSpec with BeforeAndAfterAll {

  protected lazy val icebergWarehouse = Files.createTempDirectory("iceberg-test").toString

  protected lazy val spark: SparkSession = SparkSessionBuilder.build(
    getClass.getSimpleName,
    local = true,
    additionalConfig = Option(Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
      "spark.sql.warehouse.dir" -> s"${System.getProperty("java.io.tmpdir")}/warehouse",
      "spark.sql.catalog.spark_catalog.type" -> "hadoop",
      "spark.sql.catalog.spark_catalog.warehouse" -> icebergWarehouse,
      "spark.driver.bindAddress" -> "127.0.0.1",
      "spark.ui.enabled" -> "false"
    ))
  )

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
}