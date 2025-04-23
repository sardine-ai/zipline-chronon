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

import ai.chronon.api._
import ai.chronon.spark._
import ai.chronon.spark.catalog.{DefaultFormatProvider, FormatProvider, IncompatibleSchemaException}
import ai.chronon.spark.test.TestUtils.makeDf
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, _}
import org.junit.Assert.{assertEquals, assertNull, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try

case class TestRecord(ds: String, id: String)

class SimpleAddUDF extends UDF {
  def evaluate(value: Int): Int = {
    value + 20
  }
}

class TableUtilsTest extends AnyFlatSpec {

  import ai.chronon.spark.submission

  lazy val spark: SparkSession = submission.SparkSessionBuilder.build("TableUtilsTest", local = true)
  private val tableUtils = TableTestUtils(spark)
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

  it should "handle special characters in column names with TableUtils.insertPartitions" in {
    val specialTableName = "db.special_chars_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    // Create a struct type named "with" that contains a field "dots"
    val withStructType = StructType(
      "with",
      Array(StructField("dots", IntType), StructField("id", StringType))
    )

    // Create data for our test
    val row1 = Row("value1", 42, true, Row(123, "id1"), "2023-01-01")
    val row2 = Row("value2", 84, false, Row(456, "id2"), "2023-01-02")

    // Define schema with:
    // 1. "with.dots" - a column with dots in the name
    // 2. "with" - a struct that contains a field named "dots"
    val schema = StructType(
      specialTableName,
      Array(
        StructField("normal", StringType),
        StructField("with.dots", IntType), // Column with dots
        StructField("with#hash", BooleanType), // Column with hash
        StructField("with", withStructType), // Struct named "with" with field "dots"
        StructField("ds", StringType)
      )
    )

    // Create the DataFrame with our complex schema
    val specialCharsData = makeDf(spark, schema, List(row1, row2))

    try {
      // Use TableUtils.insertPartitions with our fixed column reference handling
      tableUtils.insertPartitions(
        specialCharsData,
        specialTableName,
        partitionColumns = List("ds")
      )

      // Verify that columns were preserved correctly
      val loadedData = tableUtils.loadTable(specialTableName)
      val expectedColumns = List("normal", "with.dots", "with#hash", "with", "ds")
      assertEquals(expectedColumns, loadedData.columns.toList)

      // Verify column values including both with.dots and with.dots
      val day1Data = loadedData.where(col("ds") === "2023-01-01").collect()
      assertEquals(1, day1Data.length)
      assertEquals("value1", day1Data(0).getAs[String]("normal"))
      assertEquals(42, day1Data(0).getAs[Int]("with.dots")) // Dot column
      assertEquals(true, day1Data(0).getAs[Boolean]("with#hash"))

      // Verify the struct field "with" that contains field "dots"
      val withStruct = day1Data(0).getAs[Row]("with")
      assertEquals(123, withStruct.getAs[Int]("dots")) // Same as with.dots in dot notation
      assertEquals("id1", withStruct.getAs[String]("id"))

      // Create a DataFrame with a backtick and a column with dots and hash
      val backticksData = makeDf(
        spark,
        StructType(
          specialTableName,
          Array(
            StructField("with`backtick", StringType),
            StructField("num", IntType),
            StructField("with.hash#mix", DoubleType), // Column with both dots and hash
            StructField("ds", StringType)
          )
        ),
        List(
          Row("tick", 100, 99.9, "2023-01-03")
        )
      )

      // Test with autoExpand=true which uses our other fixed code path
      tableUtils.insertPartitions(
        backticksData,
        specialTableName,
        partitionColumns = List("ds"),
        autoExpand = true
      )

      // Verify all columns are present after expansion
      val updatedData = tableUtils.loadTable(specialTableName)
      val allExpectedCols =
        expectedColumns.reverse.tail.reverse ++ List("with`backtick", "num", "with.hash#mix") :+ "ds"
      assertEquals(allExpectedCols, updatedData.columns.toList)

      // Verify the new row data
      val day3Data = updatedData.where(col("ds") === "2023-01-03").collect()
      assertEquals(1, day3Data.length)
      assertEquals("tick", day3Data(0).getAs[String]("with`backtick"))
      assertEquals(100, day3Data(0).getAs[Int]("num"))
      assertEquals(99.9, day3Data(0).getAs[Double]("with.hash#mix"), 0.0)

      // Null for fields not in this row
      assertNull(day3Data(0).getAs[Row]("with"))
    } finally {
      // Clean up
      spark.sql(s"DROP TABLE IF EXISTS $specialTableName")
    }
  }

  it should "handle schema expansion with TableUtils.insertPartitions" in {
    val expandTableName = "db.expand_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    // Create initial DataFrame with base columns
    val initialData = spark
      .createDataFrame(
        Seq(
          (1L, "A", "2023-01-01")
        ))
      .toDF("id", "name", "ds")

    try {
      import org.junit.Assert.assertNull
      // Insert initial data
      tableUtils.insertPartitions(
        initialData,
        expandTableName,
        partitionColumns = List("ds")
      )

      // Create DataFrame with additional columns
      val expandedData = spark
        .createDataFrame(
          Seq(
            (2L, "B", Some(25), Some("user@example.com"), "2023-01-02")
          ))
        .toDF("id", "name", "age", "email", "ds")

      // Use autoExpand=true to test the column expansion logic that we fixed
      tableUtils.insertPartitions(
        expandedData,
        expandTableName,
        partitionColumns = List("ds"),
        autoExpand = true
      )

      // Verify the expanded schema
      val loadedData = tableUtils.loadTable(expandTableName)
      val expectedColumns = List("id", "name", "age", "email", "ds")
      assertEquals(expectedColumns, loadedData.columns.toList)

      // Original row should have nulls for new columns
      val day1Data = loadedData.where(col("ds") === "2023-01-01").collect()
      assertEquals(1, day1Data.length)
      assertEquals(1L, day1Data(0).getAs[Long]("id"))
      assertEquals("A", day1Data(0).getAs[String]("name"))
      assertNull(day1Data(0).getAs[Integer]("age"))
      assertNull(day1Data(0).getAs[String]("email"))

      // New row should have all columns populated
      val day2Data = loadedData.where(col("ds") === "2023-01-02").collect()
      assertEquals(1, day2Data.length)
      assertEquals(2L, day2Data(0).getAs[Long]("id"))
      assertEquals("B", day2Data(0).getAs[String]("name"))
      assertEquals(25, day2Data(0).getAs[Int]("age"))
      assertEquals("user@example.com", day2Data(0).getAs[String]("email"))
    } finally {
      // Clean up
      spark.sql(s"DROP TABLE IF EXISTS $expandTableName")
    }
  }

  it should "column from sql" in {
    val sampleSql =
      """
        |SELECT
        |  CASE WHEN column_a IS NULL THEN 1 ELSE NULL END,
        |  column_b,
        |  column_c AS not_this_one,
        |  COALESCE(IF(column_d, column_e, NULL), column_e) AS not_this_one_either,
        |  column_nested.first.second AS not_this_one_as_well
        |FROM fake_table
        |WHERE column_f IS NOT NULL AND column_g != 'Something' AND column_d > 0
        |""".stripMargin

    val columns = tableUtils.getColumnsFromQuery(sampleSql)
    val expected = Seq("column_a",
                       "column_b",
                       "column_c",
                       "column_d",
                       "column_e",
                       "column_f",
                       "column_g",
                       "column_nested.first.second").sorted
    assertEquals(expected, columns.sorted)
  }

  private def testInsertPartitions(tableName: String,
                                   df1: DataFrame,
                                   df2: DataFrame,
                                   ds1: String,
                                   ds2: String): Unit = {
    tableUtils.insertPartitions(df1, tableName, autoExpand = true)
    val addedColumns = df2.schema.fieldNames.filterNot(df1.schema.fieldNames.contains)
    val removedColumns = df1.schema.fieldNames.filterNot(df2.schema.fieldNames.contains)
    val inconsistentColumns = (
      for (
        (name1, dtype1) <- df1.schema.fields.map(structField => (structField.name, structField.dataType));
        (name2, dtype2) <- df2.schema.fields.map(structField => (structField.name, structField.dataType))
      ) yield {
        name1 == name2 && dtype1 != dtype2
      }
    ).filter(identity)

    if (inconsistentColumns.nonEmpty) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName, autoExpand = true))
      val e = insertTry.failed.get.asInstanceOf[IncompatibleSchemaException]
      assertEquals(inconsistentColumns.length, e.inconsistencies.length)
      return
    }

    if (df2.schema != df1.schema) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName))
      assertTrue(insertTry.failed.get.isInstanceOf[AnalysisException])
    }

    tableUtils.insertPartitions(df2, tableName, autoExpand = true)

    val dataRead1 = tableUtils.loadTable(tableName).where(col("ds") === ds1)
    val dataRead2 = tableUtils.loadTable(tableName).where(col("ds") === ds2)
    assertTrue(dataRead1.columns.length == dataRead2.columns.length)

    val totalColumnsCount = (df1.schema.fieldNames.toSet ++ df2.schema.fieldNames.toSet).size
    assertEquals(totalColumnsCount, dataRead1.columns.length)
    assertEquals(totalColumnsCount, dataRead2.columns.length)

    addedColumns.foreach(col => {
      dataRead1.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
    removedColumns.foreach(col => {
      dataRead2.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
  }

  it should "insert partitions add columns" in {
    val tableName = "db.test_table_1"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(4L, 5, "6", 7.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "insert partitions remove columns" in {
    val tableName = "db.test_table_2"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", 4.0, "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(5L, 6, "7", "2022-10-02")
      )
    )
    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "insert partitions modified columns" in {
    val tableName = "db.test_table_3"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", StringType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", DoubleType) // modified column data type
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, 3.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "chunk" in {
    val actual = tableUtils.chunk(Set("2021-01-01", "2021-01-02", "2021-01-05", "2021-01-07"))
    val expected = Seq(
      PartitionRange("2021-01-01", "2021-01-02"),
      PartitionRange("2021-01-05", "2021-01-05"),
      PartitionRange("2021-01-07", "2021-01-07")
    )
    assertEquals(expected, actual)
  }

  it should "drop partitions" in {
    val tableName = "db.test_drop_partitions_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("ds", StringType),
      StructField("label_ds", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
      ),
      List(
        Row(1L, 2, "2022-10-01", "2022-11-01"),
        Row(2L, 2, "2022-10-02", "2022-11-02"),
        Row(3L, 8, "2022-10-05", "2022-11-03")
      )
    )
    tableUtils.insertPartitions(df1,
                                tableName,
                                partitionColumns = List(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
    tableUtils.dropPartitions(tableName,
                              Seq("2022-10-01", "2022-10-02"),
                              subPartitionFilters = Map(Constants.LabelPartitionColumn -> "2022-11-02"))

    val updated = tableUtils.sql(s"""
         |SELECT * from $tableName
         |""".stripMargin)
    assertEquals(updated.count(), 2)
    assertTrue(
      updated
        .collect()
        .sameElements(
          List(
            Row(1L, 2, "2022-10-01", "2022-11-01"),
            Row(3L, 8, "2022-10-05", "2022-11-03")
          )))
  }

  it should "all partitions and get latest label mapping" in {
    val tableName = "db.test_show_partitions"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("ds", StringType),
      StructField("label_ds", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
      ),
      List(
        Row(1L, 2, "2022-10-01", "2022-11-01"),
        Row(2L, 2, "2022-10-02", "2022-11-02"),
        Row(3L, 8, "2022-10-05", "2022-11-05"),
        Row(1L, 2, "2022-10-01", "2022-11-09"),
        Row(2L, 2, "2022-10-02", "2022-11-09"),
        Row(3L, 8, "2022-10-05", "2022-11-09")
      )
    )
    tableUtils.insertPartitions(df1,
                                tableName,
                                partitionColumns = List(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
    val par = tableUtils.allPartitions(tableName)
    assertTrue(par.size == 6)
    assertEquals(par.head.keys, Set(tableUtils.partitionColumn, Constants.LabelPartitionColumn))

    // filter subset of partitions
    val filtered = tableUtils.allPartitions(tableName, List(Constants.LabelPartitionColumn))
    assertTrue(filtered.size == 6)
    assertEquals(filtered.head.keys, Set(Constants.LabelPartitionColumn))

    // verify the latest label version
    val labels = JoinUtils.getLatestLabelMapping(tableName, tableUtils)
    assertEquals(labels("2022-11-09"),
                 List(PartitionRange("2022-10-01", "2022-10-02"), PartitionRange("2022-10-05", "2022-10-05")))
  }

  private def prepareTestDataWithSubPartitions(tableName: String): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("ds", StringType),
      StructField("label_ds", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1
      ),
      List(
        Row(1L, "2022-11-01", "2022-11-01"),
        Row(1L, "2022-11-01", "2022-11-02"),
        Row(2L, "2022-11-02", "2022-11-02"),
        Row(1L, "2022-11-01", "2022-11-03"),
        Row(2L, "2022-11-02", "2022-11-03"),
        Row(3L, "2022-11-03", "2022-11-03")
      )
    )
    tableUtils.insertPartitions(df1,
                                tableName,
                                partitionColumns = List(tableUtils.partitionColumn, Constants.LabelPartitionColumn))

  }

  it should "last available partition" in {
    val tableName = "db.test_last_available_partition"
    prepareTestDataWithSubPartitions(tableName)
    Seq("2022-11-01", "2022-11-02", "2022-11-03").foreach { ds =>
      val firstDs = tableUtils.lastAvailablePartition(tableName, Map(Constants.LabelPartitionColumn -> ds))
      assertTrue(firstDs.contains(ds))
    }
  }

  it should "first available partition" in {
    val tableName = "db.test_first_available_partition"
    prepareTestDataWithSubPartitions(tableName)
    Seq("2022-11-01", "2022-11-02", "2022-11-03").foreach { ds =>
      val firstDs = tableUtils.firstAvailablePartition(tableName, Map(Constants.LabelPartitionColumn -> ds))
      assertTrue(firstDs.contains("2022-11-01"))
    }
  }

  it should "double udf registration" in {
    tableUtils.sql("CREATE TEMPORARY FUNCTION test AS 'ai.chronon.spark.test.SimpleAddUDF'")
    tableUtils.sql("CREATE TEMPORARY FUNCTION test AS 'ai.chronon.spark.test.SimpleAddUDF'")
  }

  it should "insert partitions table reachable already" in {
    val tableName = "db.test_table_exists_already"

    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType),
      StructField("ds", StringType)
    )

    // Create the table beforehand
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (long_field LONG, int_field INT, string_field STRING, ds STRING)")

    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )
    val df2 = makeDf(
      spark,
      StructType(
        tableName,
        columns
      ),
      List(
        Row(1L, 2, "3", "2022-10-02")
      )
    )

    // check if insertion still works
    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  it should "create table already exists" in {
    val tableName = "db.test_create_table_already_exists"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")

    val columns = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )

    spark.sql(
      "CREATE TABLE IF NOT EXISTS db.test_create_table_already_exists (long_field LONG, int_field INT, string_field STRING)")

    try {
      val df = makeDf(
        spark,
        StructType(
          tableName,
          columns
        ),
        List(
          Row(1L, 2, "3")
        )
      )
      tableUtils.createTable(df, tableName, fileFormat = "PARQUET")
      assertTrue(spark.catalog.tableExists(tableName))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "repartitioning an empty dataframe should work" in {
    import spark.implicits._
    val tableName = "db.test_empty_table"
    tableUtils.createDatabase("db")

    tableUtils.insertPartitions(spark.emptyDataset[TestRecord].toDF(), tableName)
    val res = tableUtils.loadTable(tableName)
    assertEquals(0, res.count)

    tableUtils.insertPartitions(spark.createDataFrame(List(TestRecord("2025-01-01", "a"))), tableName)
    val newRes = tableUtils.loadTable(tableName)

    assertEquals(1, newRes.count)
  }

  it should "create table" in {
    val tableName = "db.test_create_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    try {
      val columns = Array(
        StructField("long_field", LongType),
        StructField("int_field", IntType),
        StructField("string_field", StringType)
      )
      val df = makeDf(
        spark,
        StructType(
          tableName,
          columns
        ),
        List(
          Row(1L, 2, "3")
        )
      )
      tableUtils.createTable(df, tableName, fileFormat = "PARQUET")
      assertTrue(spark.catalog.tableExists(tableName))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "test catalog detection" in {
    val fp = FormatProvider.from(spark).asInstanceOf[DefaultFormatProvider]
    assertEquals("catalogA", fp.getCatalog("catalogA.foo.bar"))
    assertEquals("catalogA", fp.getCatalog("`catalogA`.foo.bar"))
    assertEquals("spark_catalog", fp.getCatalog("`catalogA.foo`.bar"))
    assertEquals("spark_catalog", fp.getCatalog("`catalogA.foo.bar`"))
    assertEquals("spark_catalog", fp.getCatalog("foo.bar"))
    assertEquals("spark_catalog", fp.getCatalog("bar"))
    assertThrows[ParseException](fp.getCatalog(""))
  }

}
