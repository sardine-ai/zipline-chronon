package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration
import com.google.cloud.hadoop.fs.gcs.HadoopConfigurationProperty
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class BigQueryCatalogTest extends AnyFunSuite with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "BigQuerySparkTest",
    local = true,
    additionalConfig = Some(
      Map(
        "spark.chronon.table.format_provider.class" -> classOf[GcpFormatProvider].getName,
        "hive.metastore.uris" -> "thrift://localhost:9083",
        "spark.chronon.partition.column" -> "c",
        "spark.hadoop.fs.gs.impl" -> classOf[GoogleHadoopFileSystem].getName,
        "spark.hadoop.fs.AbstractFileSystem.gs.impl" -> classOf[GoogleHadoopFS].getName,
        "spark.hadoop.google.cloud.auth.service.account.enable" -> true.toString
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  test("hive uris are set") {
    assertEquals("thrift://localhost:9083", spark.sqlContext.getConf("hive.metastore.uris"))
  }

  test("google runtime classes are available") {
    assertTrue(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.isInstanceOf[HadoopConfigurationProperty[Long]])
    assertCompiles("classOf[GoogleHadoopFileSystem]")
    assertCompiles("classOf[GoogleHadoopFS]")
    assertCompiles("classOf[GoogleCloudStorageFileSystem]")

  }

  test("verify dynamic classloading of GCP providers") {
    assertTrue(tableUtils.tableReadFormat("data.sample_native") match {
      case BigQueryFormat(_, _) => true
      case _            => false
    })
  }

  ignore("integration testing bigquery native table") {
    val nativeTable = "data.sample_native"
    val table = tableUtils.loadTable(nativeTable)
    table.show
    val partitioned = tableUtils.isPartitioned(nativeTable)
    println(partitioned)
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.allPartitions(nativeTable)
    println(allParts)
  }

  ignore("integration testing bigquery external table") {
    val externalTable = "data.checkouts_parquet"

    val bs = GoogleHadoopFileSystemConfiguration.BLOCK_SIZE
    println(bs)
    val table = tableUtils.loadTable(externalTable)
    table.show
    val partitioned = tableUtils.isPartitioned(externalTable)
    println(partitioned)
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.allPartitions(externalTable)
    println(allParts)
  }

  ignore("integration testing bigquery partitions") {
    // TODO(tchow): This test is ignored because it requires a running instance of the bigquery. Need to figure out stubbing locally.
    // to run this:
    //    1. Set up a tunnel to dataproc federation proxy:
    //       gcloud compute ssh zipline-canary-cluster-m \
    //        --zone us-central1-c \
    //        -- -f -N -L 9083:localhost:9083
    //    2. enable this test and off you go.
    val externalPartitions = tableUtils.partitions("data.checkouts_parquet")
    println(externalPartitions)
    val nativePartitions = tableUtils.partitions("data.sample_native")
    println(nativePartitions)
  }
}
