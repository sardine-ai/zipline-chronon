package ai.chronon.spark
import ai.chronon.api.DataModel.Events
import ai.chronon.api.{Constants, DateRange}
import ai.chronon.api.Extensions._
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.orchestration.SourceWithFilterNode
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.parseSkewKeys
import org.apache.spark.sql.functions.monotonically_increasing_id

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

/*
Runs and materializes a `Source` for a given `dateRange`. Used in the Join computation flow to first compute the Source,
then each join may have a further Bootstrap computation to produce the left side for use in the final join step.
 */
class SourceJob(node: SourceWithFilterNode, range: DateRange)(implicit tableUtils: TableUtils) {
  private val sourceWithFilter = node
  private val dateRange = range.toPartitionRange(tableUtils.partitionSpec)
  private val outputTable = node.metaData.outputTable

  def run(): Unit = {

    val source = sourceWithFilter.source

    val timeProjection = if (source.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(source.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }

    val skewKeys = parseSkewKeys(sourceWithFilter.excludeKeys)
    val skewFilter = formatFilterString(skewKeys)

    val skewFilteredSource = skewFilter
      .map(sf => {
        val copySource = source.deepCopy()
        val allFilters = source.query.wheres.asScala ++ Seq(sf)
        copySource.query.setWheres(allFilters.toJava)
        copySource
      })
      .getOrElse(source)

    // This job benefits from a step day of 1 to avoid needing to shuffle on writing output (single partition)
    dateRange.steps(days = 1).foreach { dayStep =>
      val df =
        tableUtils.scanDf(skewFilteredSource.query,
                          skewFilteredSource.table,
                          Some((Map(tableUtils.partitionColumn -> null) ++ timeProjection).toMap),
                          range = Some(dayStep))

      if (df.isEmpty) {
        throw new RuntimeException(s"Query produced 0 rows in range $dayStep.")
      }

      val dfWithTimeCol = if (source.dataModel == Events) {
        df.withTimeBasedColumn(Constants.TimePartitionColumn)
      } else {
        df
      }

      // Save using the provided outputTable or compute one if not provided
      dfWithTimeCol.save(outputTable, tableProperties = sourceWithFilter.metaData.tableProps)
    }
  }

  private def formatFilterString(keys: Option[Map[String, Seq[String]]] = None): Option[String] = {
    keys.map { keyMap =>
      keyMap
        .map { case (keyName, values) =>
          generateSkewFilterSql(keyName, values)
        }
        .filter(_.nonEmpty)
        .mkString(" OR ")
    }
  }

  def generateSkewFilterSql(key: String, values: Seq[String]): String = {
    val nulls = Seq("null", "Null", "NULL")
    val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
    val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
    (nonNullFilters ++ nullFilters).mkString(" AND ")
  }
}
