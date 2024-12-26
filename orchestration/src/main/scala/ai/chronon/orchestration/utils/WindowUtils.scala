package ai.chronon.orchestration.utils

import ai.chronon.api
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Window

object WindowUtils {
  val defaultPartitionSize: api.TimeUnit = api.TimeUnit.DAYS
  val onePartition: api.Window = new api.Window(1, defaultPartitionSize)

  def convertUnits(window: Window, offsetUnit: api.TimeUnit): Window = {
    if (window == null) return null
    if (window.timeUnit == offsetUnit) return window

    val offsetSpanMillis = new Window(1, offsetUnit).millis
    val windowLength = math.ceil(window.millis.toDouble / offsetSpanMillis.toDouble).toInt
    new Window(windowLength, offsetUnit)
  }

  def zero(timeUnits: api.TimeUnit = api.TimeUnit.DAYS): Window = new Window(0, timeUnits)
}
