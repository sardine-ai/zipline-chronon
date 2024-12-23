package ai.chronon.integrations.cloud_gcp.test

import ai.chronon.integrations.cloud_gcp.BQuery
import ai.chronon.integrations.cloud_gcp.GcpFormatProvider
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.TableUtils
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
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
        "spark.hadoop.google.cloud.auth.service.account.enable" -> true.toString,
        "spark.hadoop.fs.gs.impl" -> classOf[GoogleHadoopFileSystem].getName
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  test("hive uris are set") {
    assertEquals("thrift://localhost:9083", spark.sqlContext.getConf("hive.metastore.uris"))
  }

  test("verify dynamic classloading of GCP providers") {
    assertTrue(tableUtils.tableReadFormat("data.sample_native") match {
      case BQuery(_) => true
      case _         => false
    })
  }

  ignore("integration testing bigquery load table") {
    val externalTable = "data.checkouts_parquet"
    val table = tableUtils.loadTable(externalTable)
    tableUtils.isPartitioned(externalTable)
    tableUtils.createDatabase("test_database")
    tableUtils.allPartitions(externalTable)
    table.show
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
