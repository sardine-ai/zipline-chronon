package ai.chronon.api.test

import ai.chronon.api.ParsedTable
import org.junit.Assert.assertEquals
import org.junit.Test

class ParseTableTest {

  @Test
  def testParseTable(): Unit = {
    val testCases = List(
      ("bigquery://project.dataset.table/viewsEnabled=true,materializationDataset=<dataset>",
       ParsedTable(Some("bigquery"),
                   "project.dataset.table",
                   Map("viewsEnabled" -> "true", "materializationDataset" -> "<dataset>")),
       "should parse format, table, and multiple properties"),
      ("project.dataset.table/viewsEnabled=true,materializationDataset=<dataset>",
       ParsedTable(None,
                   "project.dataset.table",
                   Map("viewsEnabled" -> "true", "materializationDataset" -> "<dataset>")),
       "should parse table and multiple properties without format"),
      ("bigquery://project.dataset.table",
       ParsedTable(Some("bigquery"), "project.dataset.table", Map()),
       "should parse format and table without properties"),
      ("project.dataset.table", ParsedTable(None, "project.dataset.table", Map()), "should parse only table"),
      ("bigquery://project.dataset.table/",
       ParsedTable(Some("bigquery"), "project.dataset.table", Map()),
       "should parse format and table with empty properties"),
      ("project.dataset.table/",
       ParsedTable(None, "project.dataset.table", Map()),
       "should parse table with empty properties"),
      ("bigquery://project.dataset.table/singleProp=value",
       ParsedTable(Some("bigquery"), "project.dataset.table", Map("singleProp" -> "value")),
       "should parse format, table, and single property"),
      ("project.dataset.table/singleProp=value",
       ParsedTable(None, "project.dataset.table", Map("singleProp" -> "value")),
       "should parse table and single property without format")
    )

    testCases.foreach {
      case (input, expected, desc) =>
        val result = ParsedTable(input)
        assertEquals(result, expected)
        println(desc + ".. Passed")
    }
  }
}
