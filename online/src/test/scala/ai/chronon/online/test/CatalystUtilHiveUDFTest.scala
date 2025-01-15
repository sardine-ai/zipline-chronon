package ai.chronon.online.test

import ai.chronon.online.CatalystUtil
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class CatalystUtilHiveUDFTest extends AnyFlatSpec with CatalystUtilTestSparkSQLStructs with TaggedFilterSuite  {

  it should "hive ud fs via setups should work" in {
    val setups = Seq(
      "CREATE FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'",
      "CREATE FUNCTION CAT_STR AS 'ai.chronon.online.test.Cat_Str'",
    )
    val selects = Seq(
      "a" -> "MINUS_ONE(int32_x)",
      "b" -> "CAT_STR(string_x)"
    )
    val cu = new CatalystUtil(CommonScalarsStruct, selects = selects, setups = setups)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size, 2)
    assertEquals(res.get("a"), Int.MaxValue - 1)
    assertEquals(res.get("b"), "hello123")
  }

  override def tagName: String = "catalystUtilHiveUdfTest"
}
