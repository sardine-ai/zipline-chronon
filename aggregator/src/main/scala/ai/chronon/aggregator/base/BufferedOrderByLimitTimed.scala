package ai.chronon.aggregator.base

import ai.chronon.aggregator.base.ArrayTimeTuple.typ
import ai.chronon.api._

import java.util

/** A class similar to OrderByLimitTimed but using BufferedArray instead of MinHeap.
  * This implementation over-allocates the array with a buffer ratio (default 1.0),
  * which means it allocates an array of size k + k*bufferRatio.
  * When the array fills up, it sorts and discards elements > k.
  * It also maintains a max_so_far value to avoid inserting elements greater than the current max
  * when the array already has at least k elements.
  */
class BufferedOrderByLimitTimed(
    inputType: DataType,
    limit: Int,
    ordering: Ordering[ArrayTimeTuple.typ],
    bufferRatio: Double = 1.0
) extends TimedAggregator[Any, BufferedMinHeap[ArrayTimeTuple.typ], util.ArrayList[Any]] {
  type Container = BufferedMinHeap[ArrayTimeTuple.typ]

  override def outputType: DataType = ListType(inputType)

  override def irType: DataType = ListType(TimeTuple.`type`(inputType))

  override final def prepare(input: Any, ts: Long): BufferedMinHeap[ArrayTimeTuple.typ] = {
    val tuple = ArrayTimeTuple.make(ts, input)
    val arr = new BufferedMinHeap[ArrayTimeTuple.typ](limit, ordering, bufferRatio)
    arr.insert(tuple)
    arr
  }

  override final def update(state: Container, input: Any, ts: Long): Container = {
    state.insert(ArrayTimeTuple.make(ts, input)); state
  }

  override final def merge(state1: Container, state2: Container): Container = { state1.merge(state2); state1 }

  override def finalize(state: Container): util.ArrayList[Any] = {
    state.sortAndTrim()
    val inner = state.arr
    val result = new util.ArrayList[Any](inner.size())

    val it = inner.iterator()
    while (it.hasNext) {
      val timeTuple = it.next()
      val payload = timeTuple(1)
      result.add(payload)
    }

    result
  }

  override def normalize(ir: Container): util.ArrayList[Array[Any]] = {
    ir.arr
  }

  override def denormalize(ir: Any): Container = {
    val irTyped = ir.asInstanceOf[util.ArrayList[Array[Any]]]
    new Container(limit, ordering, bufferRatio, irTyped)
  }

  override def clone(ir: BufferedMinHeap[typ]): BufferedMinHeap[typ] = {
    new BufferedMinHeap[typ](limit, ordering, bufferRatio, ir.arr.clone().asInstanceOf[util.ArrayList[Array[Any]]])
  }
}

/** A more efficient implementation of LastK that uses a buffered array instead of a MinHeap.
  */
class BufferedLastK(inputType: DataType, k: Int, bufferRatio: Double = 1.0)
    extends BufferedOrderByLimitTimed(inputType, k, ArrayTimeTuple.reverse, bufferRatio)

/** A more efficient implementation of FirstK that uses a buffered array instead of a MinHeap.
  */
class BufferedFirstK(inputType: DataType, k: Int, bufferRatio: Double = 1.0)
    extends BufferedOrderByLimitTimed(inputType, k, ArrayTimeTuple, bufferRatio)
