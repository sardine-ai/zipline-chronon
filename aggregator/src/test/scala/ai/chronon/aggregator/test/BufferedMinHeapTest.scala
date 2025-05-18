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

package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.BufferedMinHeap
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

import java.util
import scala.collection.JavaConverters._

class BufferedMinHeapTest extends AnyFlatSpec {
  it should "insert elements and maintain min-heap properties" in {
    val heap = new BufferedMinHeap[Int](maxSize = 4, Ordering.Int)

    // Insert some elements
    heap.insert(5)
    heap.insert(4)
    heap.insert(9)
    heap.insert(10)

    // Should have 4 elements, all inserted
    assertEquals(4, heap.arr.size())

    // Insert another element that's smaller than current max
    heap.insert(-1)

    // Buffer allows over-allocation, so it shouldn't trigger sort/trim yet
    assertEquals(5, heap.arr.size())

    // Manually sort and verify we have the smallest elements
    heap.sortAndTrim()
    assertEquals(4, heap.arr.size())
    assertArrayEquals(Array(-1, 4, 5, 9), heap.arr.asScala.toArray)
  }

  it should "automatically sort and trim when buffer is full" in {
    // Create a heap with maxSize=3 and bufferRatio=1.0 (so totalSize=6)
    val heap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int, bufferRatio = 1.0)

    // Fill the buffer to trigger sortAndTrim
    heap.insert(8)
    heap.insert(5)
    heap.insert(10)
    heap.insert(3)
    heap.insert(7)
    heap.insert(2) // This should trigger sortAndTrim

    // After sortAndTrim, should have only maxSize elements (smallest 3)
    assertEquals(3, heap.arr.size())
    assertArrayEquals(Array(2, 3, 5), heap.arr.asScala.toArray)
  }

  it should "avoid inserting elements larger than current max after reaching maxSize" in {
    val heap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int)

    // Insert maxSize elements
    heap.insert(8)
    heap.insert(5)
    heap.insert(10)

    // Force a sort to establish maxSoFar
    heap.sortAndTrim()

    // Insert element larger than current max (10)
    heap.insert(15)

    // Should not be added
    assertEquals(3, heap.arr.size())
    assertArrayEquals(Array(5, 8, 10), heap.arr.asScala.toArray)

    // Insert element smaller than current max
    heap.insert(7)

    // Should be added
    assertEquals(4, heap.arr.size())

    // Sort and trim
    heap.sortAndTrim()
    assertEquals(3, heap.arr.size())
    assertArrayEquals(Array(5, 7, 8), heap.arr.asScala.toArray)
  }

  it should "merge two BufferedMinHeaps correctly" in {
    val heap1 = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int)
    val heap2 = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int)

    // Fill first heap
    heap1.insert(8)
    heap1.insert(5)
    heap1.insert(10)

    // Fill second heap
    heap2.insert(3)
    heap2.insert(7)
    heap2.insert(2)

    // Merge heap2 into heap1
    heap1.merge(heap2)

    // Sort and verify result
    heap1.sortAndTrim()

    // Should contain the smallest 3 elements from both heaps
    assertEquals(3, heap1.arr.size())
    assertArrayEquals(Array(2, 3, 5), heap1.arr.asScala.toArray)
  }

  it should "handle edge cases" in {
    // Test with empty heap
    val emptyHeap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int)
    emptyHeap.sortAndTrim()
    assertEquals(0, emptyHeap.arr.size())

    // Test with custom ordering (descending)
    val reverseHeap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int.reverse)
    reverseHeap.insert(2)
    reverseHeap.insert(7)
    reverseHeap.insert(4)
    reverseHeap.insert(9)
    reverseHeap.insert(1)
    reverseHeap.sortAndTrim()

    // With reverse ordering, largest 3 elements should be kept
    assertEquals(3, reverseHeap.arr.size())
    assertArrayEquals(Array(9, 7, 4), reverseHeap.arr.asScala.toArray)
  }

  it should "handle buffer ratio variations" in {
    // Small buffer ratio (tight buffer)
    val smallBufferHeap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int, bufferRatio = 0.3)
    // totalSize should be 3 + ceil(3 * 0.3) = 4

    smallBufferHeap.insert(7)
    smallBufferHeap.insert(3)
    smallBufferHeap.insert(9)
    smallBufferHeap.insert(1) // This should trigger sortAndTrim as we exceed totalSize=4

    assertEquals(3, smallBufferHeap.arr.size())
    assertArrayEquals(Array(1, 3, 7), smallBufferHeap.arr.asScala.toArray)

    // Large buffer ratio
    val largeBufferHeap = new BufferedMinHeap[Int](maxSize = 3, Ordering.Int, bufferRatio = 2.0)
    // totalSize should be 3 + ceil(3 * 2.0) = 9

    // Insert 8 elements (less than totalSize)
    // will insert 9 to 2
    for (i <- 1 to 8) {
      largeBufferHeap.insert(10 - i)
    }

    // Should not have triggered sortAndTrim yet
    assertEquals(8, largeBufferHeap.arr.size())

    // Add one more to trigger sortAndTrim
    largeBufferHeap.insert(0)

    // Should now have only maxSize elements (the smallest 3)
    assertEquals(3, largeBufferHeap.arr.size())
    assertArrayEquals(Array(0, 2, 3), largeBufferHeap.arr.asScala.toArray)
  }
}
