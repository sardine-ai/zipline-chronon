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
import ai.chronon.online.PartitionRange
import ai.chronon.online.SparkConversions
import ai.chronon.spark._
import ai.chronon.spark.test.TestUtils.makeDf
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try

case class TestRecord(ds: String, id: String)

class SimpleAddUDF extends UDF {
  def evaluate(value: Int): Int = {
    value + 20
  }
}

class TableUtilsTest extends AnyFlatSpec {
  lazy val spark: SparkSession = SparkSessionBuilder.build("TableUtilsTest", local = true)
  private val tableUtils = TableTestUtils(spark)
  private implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec

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

  it should "get field names" in {
    val schema = types.StructType(
      Seq(
        types.StructField("name", types.StringType, nullable = true),
        types.StructField("age", types.IntegerType, nullable = false),
        types.StructField("address",
                          types.StructType(
                            Seq(
                              types.StructField("street", types.StringType, nullable = true),
                              types.StructField("city", types.StringType, nullable = true)
                            )))
      )
    )
    val expectedFieldNames = Seq("name", "age", "address", "address.street", "address.city")
    val actualFieldNames = tableUtils.getFieldNames(schema)
    assertEquals(expectedFieldNames, actualFieldNames)
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

    val dataRead1 = spark.table(tableName).where(col("ds") === ds1)
    val dataRead2 = spark.table(tableName).where(col("ds") === ds2)
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
                                partitionColumns = Seq(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
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
                                partitionColumns = Seq(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
    val par = tableUtils.allPartitions(tableName)
    assertTrue(par.size == 6)
    assertEquals(par.head.keys, Set(tableUtils.partitionColumn, Constants.LabelPartitionColumn))

    // filter subset of partitions
    val filtered = tableUtils.allPartitions(tableName, Seq(Constants.LabelPartitionColumn))
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
                                partitionColumns = Seq(tableUtils.partitionColumn, Constants.LabelPartitionColumn))

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

  it should "column size estimator" in {
    val chrononType = StructType(
      "table_schema",
      Array(
        StructField("key", LongType),
        StructField("ts", LongType),
        StructField("int_field", IntType),
        StructField("array_field", ListType(IntType)),
        StructField("struct_field",
                    StructType(name = "",
                               fields = Array(
                                 StructField("double_field", DoubleType),
                                 StructField("array_field", ListType(StringType))
                               )))
      )
    )
    val sparkType = SparkConversions.fromChrononType(chrononType)
    assertEquals(
      104L,
      tableUtils.columnSizeEstimator(sparkType)
    )
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

}
