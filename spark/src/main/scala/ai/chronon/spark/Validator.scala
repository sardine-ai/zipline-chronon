package ai.chronon.spark

import ai.chronon.api.Constants
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

object Validator {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def runTimestampChecks(df: DataFrame, sampleNumber: Int = 100): Map[String, String] = {
    val hasTimestamp = df.schema.fieldNames.contains(Constants.TimeColumn)
    val mapTimestampChecks = if (hasTimestamp) {
      // set max sample to 100 rows if larger input is provided
      val sampleN = if (sampleNumber > 100) { 100 }
      else { sampleNumber }
      dataframeToMap(
        df.limit(sampleN)
          .agg(
            // will return 0 if all values are null
            sum(when(col(Constants.TimeColumn).isNull, lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("notNullCount"),
            // assumes that we have valid unix milliseconds between the date range of
            // 1971-01-01 00:00:00 (31536000000L) to 2099-12-31 23:59:59 (4102473599999L)
            // will return 0 if all values are within the range
            sum(when(col(Constants.TimeColumn).between(31536000000L, 4102473599999L), lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("badRangeCount")
          )
          .select(col("notNullCount"), col("badRangeCount"))
      )
    } else {
      Map(
        "noTsColumn" -> "No Timestamp Column"
      )
    }
    mapTimestampChecks
  }


  /** This method can be used to trigger the assertion checks
   * or print the summary stats once the timestamp checks have been run
   * @param timestampCheckMap
   * @param configType
   * @param configName
   */
  def validateTimestampChecks(timestampCheckMap: Map[String, String], configType: String, configName: String): Unit = {

    if (!timestampCheckMap.contains("noTsColumn")) {
      // do timestamp checks
      assert(
        timestampCheckMap("notNullCount") != "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has non-null timestamps.
           | check notNullCount: ${timestampCheckMap("notNullCount")}
           | """.stripMargin
      )
      assert(
        timestampCheckMap("badRangeCount") == "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has valid epoch millisecond timestamps.
           | badRangeCount: ${timestampCheckMap("badRangeCount")}
           | """.stripMargin
      )

      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check notNullCount: ${timestampCheckMap("notNullCount")}
                     |check badRangeCount: ${timestampCheckMap("badRangeCount")}
                     |""".stripMargin)

    } else {
      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check TsColumn: ${timestampCheckMap("noTsColumn")}
                     |""".stripMargin)
    }

  }

  private def dataframeToMap(inputDf: DataFrame): Map[String, String] = {
    val row: Row = inputDf.head()
    val schema = inputDf.schema
    val columns = schema.fieldNames
    val values = row.toSeq
    columns
      .zip(values)
      .map { case (column, value) =>
        (column, value.toString)
      }
      .toMap
  }
}