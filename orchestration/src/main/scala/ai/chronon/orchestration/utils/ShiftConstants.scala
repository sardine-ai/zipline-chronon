package ai.chronon.orchestration.utils

import ai.chronon.api
import ai.chronon.api.Window

object ShiftConstants {
  val noShift: Window = WindowUtils.zero()
  val shiftOne: Window = WindowUtils.onePartition
  val PartitionTimeUnit = api.TimeUnit.DAYS
}
