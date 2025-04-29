package ai.chronon.spark.catalog

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

object TableCache {

  private val dfMap: ConcurrentMap[String, DataFrame] = new ConcurrentHashMap[String, DataFrame]()

  def get(tableName: String, partitionFilters: String, fn: (String, String) => DataFrame)(implicit
      sparkSession: SparkSession): DataFrame = {

    dfMap.computeIfAbsent(tableName,
      (t: String) => {
        fn(t, partitionFilters)
      })
  }

  def remove(tableName: String): Unit = {
    dfMap.remove(tableName)
  }
}

trait Format {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def table(tableName: String, partitionFilters: String, cacheDf: Boolean = false)(implicit
      sparkSession: SparkSession): DataFrame = {

    if (cacheDf) {
      TableCache.get(tableName, partitionFilters, internalTable)
    } else {
      internalTable(tableName, partitionFilters)
    }
  }

  def internalTable(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {
    val df = sparkSession.read.table(tableName)
    if (partitionFilters.isEmpty) {
      df
    } else {
      df.where(partitionFilters)
    }
  }

  // Return the primary partitions (based on the 'partitionColumn') filtered down by sub-partition filters if provided
  // If subpartition filters are supplied and the format doesn't support it, we throw an error
  def primaryPartitions(tableName: String,
                        partitionColumn: String,
                        subPartitionsFilter: Map[String, String] = Map.empty)(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    val partitionSeq = partitions(tableName)(sparkSession)

    partitionSeq.flatMap { partitionMap =>
      if (
        subPartitionsFilter.forall { case (k, v) =>
          partitionMap.get(k).contains(v)
        }
      ) {
        partitionMap.get(partitionColumn)
      } else {
        None
      }
    }
  }

  // Return a sequence for partitions where each partition entry consists of a map of partition keys to values
  // e.g. Seq(
  //         Map("ds" -> "2023-04-01", "hr" -> "12"),
  //         Map("ds" -> "2023-04-01", "hr" -> "13")
  //         Map("ds" -> "2023-04-02", "hr" -> "00")
  //      )
  def partitions(tableName: String)(implicit sparkSession: SparkSession): List[Map[String, String]]

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

}

object Format {

  def parseHiveStylePartition(pstring: String): Map[String, String] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toMap
  }

}
