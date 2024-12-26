package ai.chronon.orchestration.utils

import ai.chronon.api.Source
import ai.chronon.orchestration.TabularDataType

object TabularDataUtils {
  def typeOf(source: Source): TabularDataType = {

    if (source.isSetJoinSource) {
      return typeOf(source.getJoinSource.getJoin.getLeft)
    }

    if (source.isSetEvents) {

      if (source.getEvents.isCumulative) {
        TabularDataType.CUMULATIVE_EVENTS
      } else {
        TabularDataType.EVENT
      }

    } else {
      TabularDataType.ENTITY
    }
  }
}
