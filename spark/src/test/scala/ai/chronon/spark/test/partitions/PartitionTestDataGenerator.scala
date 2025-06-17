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

package ai.chronon.spark.test.partitions

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random

object PartitionTestDataGenerator {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  
  case class TableConfig(
      tableName: String,
      partitionColumns: List[String],
      numRows: Int,
      dateRange: (String, String),
      additionalColumns: Map[String, String] = Map.empty
  )

  def generateTestTables(spark: SparkSession, outputPath: String): Unit = {
    logger.info(s"Generating test tables in $outputPath")
    
    val configs = List(
      // Simple date partitioned table
      TableConfig(
        tableName = "events_daily",
        partitionColumns = List("ds"),
        numRows = 10000,
        dateRange = ("2023-01-01", "2023-01-31"),
        additionalColumns = Map(
          "user_id" -> "long",
          "event_type" -> "string",
          "value" -> "double"
        )
      ),
      
      // Multi-level partitioned table
      TableConfig(
        tableName = "user_activity",
        partitionColumns = List("ds", "region", "platform"),
        numRows = 5000,
        dateRange = ("2023-01-01", "2023-01-15"),
        additionalColumns = Map(
          "user_id" -> "long",
          "activity_type" -> "string",
          "duration_seconds" -> "int",
          "metadata" -> "string"
        )
      ),
      
      // Hourly partitioned table
      TableConfig(
        tableName = "transactions_hourly",
        partitionColumns = List("date_hour"),
        numRows = 8000,
        dateRange = ("2023-01-01", "2023-01-07"),
        additionalColumns = Map(
          "transaction_id" -> "string",
          "merchant_id" -> "long",
          "amount" -> "decimal(10,2)",
          "currency" -> "string",
          "status" -> "string"
        )
      ),
      
      // Complex nested partitions
      TableConfig(
        tableName = "product_metrics",
        partitionColumns = List("year", "month", "day", "category"),
        numRows = 3000,
        dateRange = ("2023-01-01", "2023-01-10"),
        additionalColumns = Map(
          "product_id" -> "string",
          "views" -> "long",
          "clicks" -> "long",
          "conversions" -> "long",
          "revenue" -> "double"
        )
      )
    )
    
    configs.foreach { config =>
      generateTable(spark, outputPath, config)
    }
  }

  private def generateTable(spark: SparkSession, outputPath: String, config: TableConfig): Unit = {
    logger.info(s"Generating table: ${config.tableName}")
    
    import spark.implicits._
    
    val baseData = spark.range(1, config.numRows + 1)
      .withColumn("id", col("id"))
    
    // Add partition columns
    val dataWithPartitions = addPartitionColumns(baseData, config)
    
    // Add additional columns
    val finalData = addAdditionalColumns(dataWithPartitions, config)
    
    // Write the table
    val tablePath = s"$outputPath/${config.tableName}"
    finalData.write
      .mode(SaveMode.Overwrite)
      .partitionBy(config.partitionColumns: _*)
      .option("path", tablePath)
      .saveAsTable(config.tableName)
    
    // Show some stats
    val partitionCount = finalData.select(config.partitionColumns.map(col): _*).distinct().count()
    logger.info(s"Created ${config.tableName}: ${config.numRows} rows, $partitionCount partitions")
    
    // Show sample partitions
    val samplePartitions = finalData.select(config.partitionColumns.map(col): _*)
      .distinct()
      .limit(5)
      .collect()
      .map(_.mkString(", "))
    
    logger.info(s"Sample partitions: ${samplePartitions.mkString("; ")}")
  }

  private def addPartitionColumns(df: DataFrame, config: TableConfig): DataFrame = {
    val startDate = LocalDate.parse(config.dateRange._1)
    val endDate = LocalDate.parse(config.dateRange._2)
    val daysBetween = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt + 1
    
    var result = df
    
    config.partitionColumns.foreach {
      case "ds" =>
        // Daily partitions
        result = result.withColumn("ds", 
          date_add(lit(config.dateRange._1), (col("id") % daysBetween).cast(IntegerType))
            .cast(StringType)
        )
        
      case "date_hour" =>
        // Hourly partitions
        result = result.withColumn("date_hour",
          concat(
            date_add(lit(config.dateRange._1), (col("id") % daysBetween).cast(IntegerType)),
            lit("-"),
            format_string("%02d", col("id") % 24)
          )
        )
        
      case "region" =>
        result = result.withColumn("region",
          when(col("id") % 4 === 0, "us-west")
          .when(col("id") % 4 === 1, "us-east")
          .when(col("id") % 4 === 2, "europe")
          .otherwise("asia")
        )
        
      case "platform" =>
        result = result.withColumn("platform",
          when(col("id") % 3 === 0, "web")
          .when(col("id") % 3 === 1, "mobile")
          .otherwise("api")
        )
        
      case "year" =>
        result = result.withColumn("year", year(
          date_add(lit(config.dateRange._1), (col("id") % daysBetween).cast(IntegerType))
        ))
        
      case "month" =>
        result = result.withColumn("month", month(
          date_add(lit(config.dateRange._1), (col("id") % daysBetween).cast(IntegerType))
        ))
        
      case "day" =>
        result = result.withColumn("day", dayofmonth(
          date_add(lit(config.dateRange._1), (col("id") % daysBetween).cast(IntegerType))
        ))
        
      case "category" =>
        result = result.withColumn("category",
          when(col("id") % 5 === 0, "electronics")
          .when(col("id") % 5 === 1, "clothing")
          .when(col("id") % 5 === 2, "books")
          .when(col("id") % 5 === 3, "home")
          .otherwise("sports")
        )
        
      case other =>
        logger.warn(s"Unknown partition column: $other")
    }
    
    result
  }

  private def addAdditionalColumns(df: DataFrame, config: TableConfig): DataFrame = {
    var result = df
    
    config.additionalColumns.foreach {
      case ("user_id", "long") =>
        result = result.withColumn("user_id", (col("id") % 1000) + 1)
        
      case ("event_type", "string") =>
        result = result.withColumn("event_type",
          when(col("id") % 4 === 0, "click")
          .when(col("id") % 4 === 1, "view")
          .when(col("id") % 4 === 2, "purchase")
          .otherwise("search")
        )
        
      case ("value", "double") =>
        result = result.withColumn("value", rand() * 1000)
        
      case ("activity_type", "string") =>
        result = result.withColumn("activity_type",
          when(col("id") % 3 === 0, "login")
          .when(col("id") % 3 === 1, "logout")
          .otherwise("action")
        )
        
      case ("duration_seconds", "int") =>
        result = result.withColumn("duration_seconds", (rand() * 3600).cast(IntegerType))
        
      case ("metadata", "string") =>
        result = result.withColumn("metadata", concat(lit("meta_"), col("id")))
        
      case ("transaction_id", "string") =>
        result = result.withColumn("transaction_id", concat(lit("txn_"), col("id")))
        
      case ("merchant_id", "long") =>
        result = result.withColumn("merchant_id", (col("id") % 100) + 1)
        
      case ("amount", "decimal(10,2)") =>
        result = result.withColumn("amount", (rand() * 1000).cast(DecimalType(10, 2)))
        
      case ("currency", "string") =>
        result = result.withColumn("currency",
          when(col("id") % 3 === 0, "USD")
          .when(col("id") % 3 === 1, "EUR")
          .otherwise("GBP")
        )
        
      case ("status", "string") =>
        result = result.withColumn("status",
          when(col("id") % 5 === 0, "completed")
          .when(col("id") % 5 === 1, "pending")
          .when(col("id") % 5 === 2, "failed")
          .when(col("id") % 5 === 3, "cancelled")
          .otherwise("processing")
        )
        
      case ("product_id", "string") =>
        result = result.withColumn("product_id", concat(lit("prod_"), col("id") % 500))
        
      case ("views", "long") =>
        result = result.withColumn("views", (rand() * 10000).cast(LongType))
        
      case ("clicks", "long") =>
        result = result.withColumn("clicks", (rand() * 1000).cast(LongType))
        
      case ("conversions", "long") =>
        result = result.withColumn("conversions", (rand() * 100).cast(LongType))
        
      case ("revenue", "double") =>
        result = result.withColumn("revenue", rand() * 5000)
        
      case (colName, colType) =>
        logger.warn(s"Unknown column type: $colName -> $colType")
    }
    
    result
  }

  def main(args: Array[String]): Unit = {
    val outputPath = if (args.length > 0) args(0) else "/tmp/chronon_test_tables"
    
    val spark = SparkSession.builder()
      .appName("Partition Test Data Generator")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", s"$outputPath/warehouse")
      .getOrCreate()
    
    try {
      generateTestTables(spark, outputPath)
      logger.info("Test data generation completed successfully")
    } finally {
      spark.stop()
    }
  }
}