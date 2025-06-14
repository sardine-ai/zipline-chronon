package ai.chronon.api.test.planner

import ai.chronon.api.{Join, PartitionSpec}
import ai.chronon.api.planner.LocalRunner
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.planner.MonolithJoinPlanner
import scala.jdk.CollectionConverters._

import java.nio.file.Paths

class PlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec = PartitionSpec.daily

  it should "monolith join planner plans valid confs without exceptions" in {

    val runfilesDir = System.getenv("RUNFILES_DIR")
    val rootDir = Paths.get(runfilesDir, "chronon/spark/src/test/resources/canary/compiled/joins")

    val joinConfs = LocalRunner.parseConfs[Join](rootDir.toString)

    val joinPlanners = joinConfs.map(MonolithJoinPlanner(_))

    joinPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          plan.terminalNodeNames.asScala.size should be > 0
        }
      }
  }

}
