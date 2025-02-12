package ai.chronon.spark
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.PartitionRange
import ai.chronon.orchestration.SourceWithFilter
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.parseSkewKeys

import scala.jdk.CollectionConverters.asScalaBufferConverter

/*
Runs and materializes a `Source` for a given `dateRange`. Used in the Join computation flow to first compute the Source,
then each join may have a further Bootstrap computation to produce the left side to effectively use.
 */
class SourceJob(sourceWithFilter: SourceWithFilter, tableUtils: TableUtils, range: PartitionRange) {

  def run(): Unit = {

    val source = sourceWithFilter.source

    val timeProjection = if (source.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(source.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }

    val skewKeys = parseSkewKeys(sourceWithFilter.excludeKeys)
    val skewFilter = formatFilterString(Option(skewKeys))

    val skewFilteredSource = skewFilter
      .map(sf => {
        val copySource = source.deepCopy()
        val allFilters = source.query.wheres.asScala ++ Seq(sf)
        copySource.query.setWheres(allFilters.toJava)
        copySource
      })
      .getOrElse(source)

    val df = tableUtils.scanDf(skewFilteredSource.query,
                               skewFilteredSource.table,
                               Some(Map(tableUtils.partitionColumn -> null) ++ timeProjection),
                               range = Some(range))

    if (df.isEmpty) {
      throw new RuntimeException(s"Query produced 0 rows in range $range.")
    }

    val outputName =
      f"${source.table}_${ThriftJsonCodec.md5Digest(sourceWithFilter)}" // Or should we pass this in as an arg?

    df.save(outputName)
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
