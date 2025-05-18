package ai.chronon.aggregator.base

import ai.chronon.api.ScalaJavaConversions.IteratorOps

import java.util

/** An implementation of a fast min-element collection using an over-allocated array.
  * The array has size k + k*bufferRatio, and when it fills up, we sort and discard elements > k.
  * We also maintain a max_so_far value to avoid unnecessary insertions.
  *
  * This essentially computes smallest_k
  */
class BufferedMinHeap[T](maxSize: Int,
                         ordering: Ordering[T],
                         bufferRatio: Double = 1.0,
                         val arr: util.ArrayList[T] = new util.ArrayList[T]()) {

  private val totalSize = maxSize + Math.ceil(maxSize * bufferRatio).toInt
  private var maxSoFar: Option[T] = None

  def insert(elem: T): Unit = {

    if (arr.size >= maxSize) {

      // on serde max_so_far could be lost - re-create it
      if (maxSoFar.isEmpty) {
        maxSoFar = Some(arr.iterator().toScala.max(ordering))
      }

      // check against maxSoFar to avoid unnecessary insertions
      if (ordering.gteq(elem, maxSoFar.get)) {
        return
      }
    }

    arr.add(elem)

    // if we've filled the buffer, sort and trim the array
    if (arr.size >= totalSize) {
      sortAndTrim()
    }
  }

  def sortAndTrim(): Unit = {
    // todo - quickSelect - no need to sort the whole array
    arr.sort(ordering)

    // Trim the array to keep only the top k elements
    while (arr.size() > maxSize) {
      arr.remove(arr.size() - 1)
    }

    // Update maxSoFar after trimming to the last element (which will be the max)
    if (arr.size() > 0) {
      maxSoFar = Some(arr.get(arr.size() - 1))
    } else {
      maxSoFar = None
    }

  }

  def merge(other: BufferedMinHeap[T]): Unit = {

    val it = other.arr.iterator
    while (it.hasNext) {
      insert(it.next())
    }

  }

}
