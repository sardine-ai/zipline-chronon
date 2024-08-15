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

package ai.chronon.api

// utilized by both streaming and batch
object QueryUtils {

  def buildSelects(selects: Map[String, String], fillIfAbsent: Option[Map[String, String]] = None): Seq[String] = {

    def toProjections(m: Map[String, String]): Seq[String] =
      m.map {
        case (col, expr) => if ((expr == col) || (expr == null)) s"`$col`" else s"$expr as `$col`"
      }.toSeq

    (Option(selects), fillIfAbsent) match {
      // pick only aliases with valid expression from the fills
      // eg., select *, ts from x -- is not valid, ts will be ambiguous & double selected with same name
      // but select *, unixtime(ds) as `ts` from x -- is valid
      case (Some(sels), Some(fills)) if sels.isEmpty => Seq("*") ++ toProjections(fills.filter(_._2 != null))
      case (Some(sels), Some(fills))                 => toProjections(fills ++ sels)
      case (Some(sels), None)                        => toProjections(sels)
      case (None, _)                                 => Seq("*")
    }
  }

  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Map[String, String],
            from: String,
            wheres: scala.collection.Seq[String],
            fillIfAbsent: Option[Map[String, String]] = None): String = {

    val finalSelects = buildSelects(selects, fillIfAbsent)

    val whereClause = Option(wheres)
      .filter(_.nonEmpty)
      .map { ws =>
        s"""
           |WHERE
           |  ${ws.map(w => s"(${w})").mkString(" AND ")}""".stripMargin
      }
      .getOrElse("")

    s"""SELECT
       |  ${finalSelects.mkString(",\n  ")}
       |FROM $from $whereClause""".stripMargin
  }
}
