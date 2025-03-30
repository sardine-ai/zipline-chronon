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

package ai.chronon.aggregator.row

import ai.chronon.aggregator.base.BaseAggregator
import ai.chronon.api.Row

import scala.collection.mutable

class BucketedColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                  columnIndicesArg: ColumnIndices,
                                                  bucketIndex: Int,
                                                  rowUpdater: Dispatcher[Input, Any])
    extends MapColumnAggregatorBase(agg) {

  override val columnIndices: ColumnIndices = columnIndicesArg

  def guardedApply(currentIr: Any, inputRow: Row, prepareFunc: Row => Any, updateFunc: (Any, Row) => Any): Any = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return currentIr // null inputs are ignored

    val bucketVal = inputRow.get(bucketIndex)
    if (bucketVal == null) return currentIr // null buckets are ignored

    val bucket = bucketVal.asInstanceOf[String]

    lazy val prepared = prepareFunc(inputRow)

    if (prepared == null) return currentIr

    val map = if (currentIr == null) { // map is absent - create with prepared
      new java.util.HashMap[String, IR]()
    } else {
      castIr(currentIr)
    }

    if (!map.containsKey(bucket)) { // bucket is absent - so init
      map.put(bucket, prepared.asInstanceOf[IR])
    } else {
      val updated = updateFunc(map.get(bucket), inputRow)
      if (updated != null) map.put(bucket, updated.asInstanceOf[IR])
    }

    map
  }

  override def updateCol(currentIr: Any, inputRow: Row): Any = {
    guardedApply(currentIr, inputRow, rowUpdater.prepare, rowUpdater.updateColumn)
  }

  override def deleteCol(currentIr: Any, inputRow: Row): Any = {
    if (!agg.isDeletable) return currentIr
    guardedApply(currentIr, inputRow, rowUpdater.inversePrepare, rowUpdater.deleteColumn)
  }
}
