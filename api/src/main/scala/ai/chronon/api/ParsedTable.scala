package ai.chronon.api

// options are used when reading the underlying data-source
// there could be filters & predicates you want to push down to the data-source
case class ParsedTable(format: Option[String], table: String, options: Map[String, String] = Map.empty)

object ParsedTable {
  def apply(t: String): ParsedTable = {
    val parts = t.split("://", 2)

    val (format, rest) = if (parts.length == 2) (Some(parts(0)), parts(1)) else (None, t)

    val tableParts = rest.split("/", 2)
    val table = tableParts(0)

    val conf = if (tableParts.length == 2) {
      tableParts(1)
        .split(',')
        .flatMap { prop =>
          val keyValue = prop.split("=", 2)
          if (keyValue.length == 2 && keyValue(0).trim.nonEmpty) {
            Some(keyValue(0).trim -> keyValue(1).trim)
          } else {
            None
          }
        }
        .toMap
    } else {
      Map.empty[String, String]
    }

    ParsedTable(format, table, conf)
  }
}
