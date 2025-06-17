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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object TestDataCreator {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def createTestTables(warehouseDir: String): Unit = {
    logger.info(s"Creating test tables in warehouse: $warehouseDir")
    
    val spark = SparkSession.builder()
      .appName("Test Data Creator")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test_events table
      logger.info("Creating test_events table...")
      val eventsData = Seq(
        (1L, 101L, "click", 1.5, "2023-01-01"),
        (2L, 102L, "view", 2.0, "2023-01-01"),
        (3L, 103L, "search", 0.5, "2023-01-01"),
        (4L, 104L, "click", 1.8, "2023-01-02"),
        (5L, 105L, "purchase", 99.99, "2023-01-02"),
        (6L, 106L, "view", 0.0, "2023-01-02"),
        (7L, 107L, "search", 1.2, "2023-01-03"),
        (8L, 108L, "click", 2.1, "2023-01-03"),
        (9L, 109L, "purchase", 149.99, "2023-01-04"),
        (10L, 110L, "view", 3.2, "2023-01-04")
      ).toDF("id", "user_id", "event_type", "value", "ds")

      eventsData.write
        .mode(SaveMode.Overwrite)
        .partitionBy("ds")
        .option("path", s"$warehouseDir/test_events")
        .saveAsTable("test_events")

      logger.info("Created test_events with partitions:")
      spark.sql("SHOW PARTITIONS test_events").show()

      // Create user_activity table
      logger.info("Creating user_activity table...")
      val userActivityData = Seq(
        (201L, "login", 120, "2023-01-01", "us-west"),
        (202L, "browse", 300, "2023-01-01", "us-west"),
        (203L, "login", 90, "2023-01-01", "us-east"),
        (204L, "purchase", 600, "2023-01-01", "us-east"),
        (205L, "search", 45, "2023-01-02", "us-west"),
        (206L, "logout", 5, "2023-01-02", "us-west"),
        (207L, "login", 75, "2023-01-02", "europe"),
        (208L, "browse", 450, "2023-01-02", "europe"),
        (209L, "purchase", 800, "2023-01-03", "us-west"),
        (210L, "search", 30, "2023-01-03", "asia")
      ).toDF("user_id", "activity", "duration_seconds", "ds", "region")

      userActivityData.write
        .mode(SaveMode.Overwrite)
        .partitionBy("ds", "region")
        .option("path", s"$warehouseDir/user_activity")
        .saveAsTable("user_activity")

      logger.info("Created user_activity with partitions:")
      spark.sql("SHOW PARTITIONS user_activity").show()

      // Show table summaries
      logger.info("Tables created:")
      spark.sql("SHOW TABLES").show()

      logger.info("test_events summary:")
      spark.sql("SELECT ds, COUNT(*) as count FROM test_events GROUP BY ds ORDER BY ds").show()

      logger.info("user_activity summary:")
      spark.sql("SELECT ds, region, COUNT(*) as count FROM user_activity GROUP BY ds, region ORDER BY ds, region").show()

      logger.info("Test data creation completed successfully!")

    } finally {
      spark.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    val warehouseDir = if (args.length > 0) args(0) else "/tmp/chronon_test_warehouse"
    logger.info(s"Starting test data creation with warehouse: $warehouseDir")
    
    try {
      createTestTables(warehouseDir)
      System.exit(0)
    } catch {
      case e: Exception =>
        logger.error("Failed to create test data", e)
        System.exit(1)
    }
  }
}