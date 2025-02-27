package ai.chronon.integrations.aws


import ai.chronon.spark.{SparkSessionBuilder, TableUtils, ChrononHudiKryoRegistrator}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class GlueCatalogTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    classOf[GlueCatalogTest].getSimpleName,
    local = true,
    additionalConfig = Some(
      Map(
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
         "spark.kryo.registrator"-> classOf[ChrononHudiKryoRegistrator].getName,
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  "basic round trip hudi table" should "work with local metastore" in {
    import spark.implicits._

    val input = Set(1,2,3,4)
    val sourceDF = spark.sparkContext.parallelize(input.toSeq).toDF("id")

    sourceDF.write.format("hudi")
      .mode(SaveMode.Overwrite)
      .saveAsTable("test_hudi_table")

    val back = spark.table("test_hudi_table").select("id").as[Int].collect()
    assertEquals(input, back.toSet)

  }
}
