package ai.chronon.online.test

import ai.chronon.online.CatalystUtil
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class CatalystUtilHiveUDFTest extends AnyFlatSpec with CatalystUtilTestSparkSQLStructs {

  "catalyst util" should "work with hive_udfs via setups should work" in {
    val setups = Seq(
      "CREATE FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'",
      "CREATE FUNCTION CAT_STR AS 'ai.chronon.online.test.Cat_Str'"
    )
    val selects = Seq(
      "a" -> "MINUS_ONE(int32_x)",
      "b" -> "CAT_STR(string_x)"
    )
    val cu = new CatalystUtil(CommonScalarsStruct, selects = selects, setups = setups)
    val resList = cu.performSql(CommonScalarsRow)
    assertEquals(resList.size, 1)
    val resMap = resList.head
    assertEquals(resMap.size, 2)
    assertEquals(resMap("a"), Int.MaxValue - 1)
    assertEquals(resMap("b"), "hello123")
  }
}
