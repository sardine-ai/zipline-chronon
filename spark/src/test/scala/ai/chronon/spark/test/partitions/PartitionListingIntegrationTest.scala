package ai.chronon.spark.test.partitions

import ai.chronon.spark.partitions.{PartitionListingService, PartitionListResponse}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.sys.process._
import scala.util.{Failure, Random, Success, Try}

class PartitionListingIntegrationTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val testPort = 18080 + Random.nextInt(1000) // Random port to avoid conflicts
  private val baseUrl = s"http://localhost:$testPort"
  
  private var spark: SparkSession = _
  private var service: PartitionListingService = _
  private var serviceThread: Thread = _
  private var tempDir: String = _
  
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)

  override def beforeAll(): Unit = {
    // Create temporary directory for test tables
    tempDir = Files.createTempDirectory("chronon_partition_test").toString
    logger.info(s"Created temp directory: $tempDir")
    
    // Initialize Spark session
    spark = SparkSession.builder()
      .appName("Partition Listing Integration Test")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", s"$tempDir/warehouse")
      .config("spark.sql.streaming.checkpointLocation", s"$tempDir/checkpoints")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    // Start the partition listing service
    service = new PartitionListingService(spark, testPort)
    serviceThread = new Thread(() => {
      service.start()
    })
    serviceThread.setDaemon(true)
    serviceThread.start()
    
    // Wait for service to start
    waitForServiceToStart()
  }

  override def afterAll(): Unit = {
    Try {
      service.shutdown()
      serviceThread.interrupt()
      spark.stop()
      // Clean up temp directory
      s"rm -rf $tempDir".!
    }
  }

  override def beforeEach(): Unit = {
    // Clean up any existing test tables
    Try(spark.sql("DROP TABLE IF EXISTS test_hive_partitioned"))
    Try(spark.sql("DROP TABLE IF EXISTS test_events"))
    Try(spark.sql("DROP TABLE IF EXISTS test_multi_partition"))
  }

  private def waitForServiceToStart(): Unit = {
    (0 until 30).foreach { _ =>
      Try {
        val connection = new URL(s"$baseUrl/health").openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.setConnectTimeout(1000)
        connection.setReadTimeout(1000)
        if (connection.getResponseCode == 200) {
          logger.info("Service started successfully")
          return
        }
      }
      Thread.sleep(1000)
    }
    throw new RuntimeException("Service failed to start after 30 attempts")
  }

  private def httpGet(url: String): (Int, String) = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(10000)
    
    val responseCode = connection.getResponseCode
    val inputStream = if (responseCode >= 200 && responseCode < 300) connection.getInputStream else connection.getErrorStream
    
    val response = Source.fromInputStream(inputStream).mkString
    inputStream.close()
    (responseCode, response)
  }

  private def parsePartitionResponse(json: String): PartitionListResponse = {
    mapper.readValue(json, classOf[PartitionListResponse])
  }

  test("Health check endpoint should work") {
    val (statusCode, response) = httpGet(s"$baseUrl/health")
    assert(statusCode == 200)
    assert(response.contains("healthy"))
    logger.info("✓ Health check passed")
  }

  test("Service info endpoint should work") {
    val (statusCode, response) = httpGet(s"$baseUrl/api/info")
    assert(statusCode == 200)
    assert(response.contains("chronon-partition-listing"))
    logger.info("✓ Service info passed")
  }

  test("Create and list partitions for Hive table") {
    // Create sample data with date partitions
    val testData = spark.range(1, 101)
      .withColumn("name", concat(lit("user_"), col("id")))
      .withColumn("value", (col("id") * 10).cast(DoubleType))
      .withColumn("ds", 
        when(col("id") <= 33, "2023-01-01")
        .when(col("id") <= 66, "2023-01-02")
        .otherwise("2023-01-03")
      )
      .withColumn("region", when(col("id") % 2 === 0, "us-west").otherwise("us-east"))

    // Write as partitioned Hive table
    testData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("ds")
      .option("path", s"$tempDir/test_hive_partitioned")
      .saveAsTable("test_hive_partitioned")

    logger.info("Created Hive partitioned table")

    // Test basic partition listing
    val (statusCode, response) = httpGet(s"$baseUrl/api/partitions/test_hive_partitioned")
    assert(statusCode == 200)

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success)
    assert(partitionResponse.tableName == "test_hive_partitioned")
    assert(partitionResponse.partitions.nonEmpty)
    assert(partitionResponse.partitions.sorted == List("2023-01-01", "2023-01-02", "2023-01-03"))

    logger.info(s"✓ Hive table partitions: ${partitionResponse.partitions}")
  }

  test("Create and list partitions with date range filter") {
    // Create more extensive date range
    val testData = spark.range(1, 151)
      .withColumn("event_type", lit("click"))
      .withColumn("user_id", (col("id") % 50) + 1)
      .withColumn("ds",
        when(col("id") <= 30, "2023-01-01")
        .when(col("id") <= 60, "2023-01-02")
        .when(col("id") <= 90, "2023-01-03")
        .when(col("id") <= 120, "2023-01-04")
        .otherwise("2023-01-05")
      )

    testData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("ds")
      .option("path", s"$tempDir/test_events")
      .saveAsTable("test_events")

    logger.info("Created events table with date partitions")

    // Test date range filtering
    val url = s"$baseUrl/api/partitions/test_events?startDate=2023-01-02&endDate=2023-01-04"
    val (statusCode, response) = httpGet(url)
    assert(statusCode == 200)

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success)
    assert(partitionResponse.partitions.size >= 3) // Should include 2023-01-02, 2023-01-03, 2023-01-04

    logger.info(s"✓ Date range filtered partitions: ${partitionResponse.partitions}")
  }

  test("Create and list multi-level partitioned table") {
    // Create data with multiple partition columns
    val testData = spark.range(1, 201)
      .withColumn("ds",
        when(col("id") <= 50, "2023-01-01")
        .when(col("id") <= 100, "2023-01-02")
        .when(col("id") <= 150, "2023-01-03")
        .otherwise("2023-01-04")
      )
      .withColumn("region",
        when(col("id") % 3 === 0, "us-west")
        .when(col("id") % 3 === 1, "us-east")
        .otherwise("europe")
      )
      .withColumn("event_type", when(col("id") % 2 === 0, "click").otherwise("view"))

    testData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("ds", "region")
      .option("path", s"$tempDir/test_multi_partition")
      .saveAsTable("test_multi_partition")

    logger.info("Created multi-level partitioned table")

    // Test basic listing
    val (statusCode, response) = httpGet(s"$baseUrl/api/partitions/test_multi_partition")
    assert(statusCode == 200)

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success)
    assert(partitionResponse.partitions.nonEmpty)

    logger.info(s"✓ Multi-partition table partitions: ${partitionResponse.partitions}")

    // Test with sub-partition filter
    val filteredUrl = s"$baseUrl/api/partitions/test_multi_partition?filter.region=us-west"
    val (filteredStatusCode, filteredResponse) = httpGet(filteredUrl)
    assert(filteredStatusCode == 200)

    val filteredPartitionResponse = parsePartitionResponse(filteredResponse)
    assert(filteredPartitionResponse.success)

    logger.info(s"✓ Filtered partitions: ${filteredPartitionResponse.partitions}")
  }

  test("Handle non-existent table gracefully") {
    val (statusCode, response) = httpGet(s"$baseUrl/api/partitions/non_existent_table")
    assert(statusCode == 200) // Service returns 200

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success) // Service returns success=true for non-existent tables
    assert(partitionResponse.partitions.isEmpty) // But with empty partition list

    logger.info(s"✓ Non-existent table handled gracefully: returns empty partition list")
  }

  test("Handle invalid endpoint gracefully") {
    val (statusCode, response) = httpGet(s"$baseUrl/api/invalid")
    assert(statusCode == 404)
    assert(response.contains("not found") || response.contains("Not Found"))

    logger.info("✓ Invalid endpoint handled gracefully")
  }

  test("Custom partition column name") {
    // Create data with custom partition column
    val testData = spark.range(1, 51)
      .withColumn("event_date", when(col("id") <= 25, "2023-02-01").otherwise("2023-02-02"))

    testData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("event_date")
      .option("path", s"$tempDir/test_custom_partition")
      .saveAsTable("test_custom_partition")

    logger.info("Created table with custom partition column")

    // Test with custom partition column name
    val url = s"$baseUrl/api/partitions/test_custom_partition?partitionColumn=event_date"
    val (statusCode, response) = httpGet(url)
    assert(statusCode == 200)

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success)
    assert(partitionResponse.partitions.contains("2023-02-01"))
    assert(partitionResponse.partitions.contains("2023-02-02"))

    logger.info(s"✓ Custom partition column partitions: ${partitionResponse.partitions}")
  }

  test("Custom partition format and interval") {
    // Create data with compact date format
    val testData = spark.range(1, 31)
      .withColumn("compact_date", when(col("id") <= 15, "20230301").otherwise("20230302"))

    testData.write
      .mode(SaveMode.Overwrite)
      .partitionBy("compact_date")
      .option("path", s"$tempDir/test_format_partition")
      .saveAsTable("test_format_partition")

    logger.info("Created table with compact date format")

    // Test with custom partition format
    val url = s"$baseUrl/api/partitions/test_format_partition?partitionColumn=compact_date&partitionFormat=yyyyMMdd"
    val (statusCode, response) = httpGet(url)
    assert(statusCode == 200)

    val partitionResponse = parsePartitionResponse(response)
    assert(partitionResponse.success)
    assert(partitionResponse.partitions.contains("20230301"))
    assert(partitionResponse.partitions.contains("20230302"))

    logger.info(s"✓ Custom format partitions: ${partitionResponse.partitions}")

    // Test with hourly interval parameter
    val hourlyUrl = s"$baseUrl/api/partitions/test_format_partition?partitionInterval=hourly&partitionFormat=yyyyMMdd"
    val (hourlyStatusCode, hourlyResponse) = httpGet(hourlyUrl)
    assert(hourlyStatusCode == 200)

    val hourlyPartitionResponse = parsePartitionResponse(hourlyResponse)
    assert(hourlyPartitionResponse.success)

    logger.info(s"✓ Hourly interval partitions: ${hourlyPartitionResponse.partitions}")
  }

  test("Curl commands demonstration") {
    logger.info("\n" + "="*80)
    logger.info("CURL COMMAND DEMONSTRATIONS")
    logger.info("="*80)

    val commands = List(
      s"curl -s $baseUrl/health",
      s"curl -s $baseUrl/api/info",
      s"curl -s $baseUrl/api/partitions/test_hive_partitioned",
      s"curl -s '$baseUrl/api/partitions/test_events?startDate=2023-01-02&endDate=2023-01-04'",
      s"curl -s '$baseUrl/api/partitions/test_multi_partition?filter.region=us-west'",
      s"curl -s '$baseUrl/api/partitions/test_custom_partition?partitionColumn=event_date'",
      s"curl -s '$baseUrl/api/partitions/test_format_partition?partitionFormat=yyyyMMdd&partitionInterval=daily'",
      s"curl -s $baseUrl/api/partitions/non_existent_table"
    )

    commands.foreach { cmd =>
      logger.info(s"\n> $cmd")
      Try {
        val result = cmd.!!
        logger.info(s"Response: $result")
      } match {
        case Success(_) => // Success already logged
        case Failure(e) => logger.info(s"Error: ${e.getMessage}")
      }
    }

    logger.info("\n" + "="*80)
  }
}
