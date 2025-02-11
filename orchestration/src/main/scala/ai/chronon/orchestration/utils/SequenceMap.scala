package ai.chronon.orchestration.utils

import scala.collection.mutable

/** [[SequenceMap]] maintains a sequence of unique values for each key
  * while preserving the first insertion order and returning the index of the first-insertion.
  *
  * [[RepoIndex]] needs to compute versions of the whole repo on every call to its [[addNodes]] method.
  * This map ensures that version assignment is fast.
  *
  * NOTE: Methods of this class are not thread safe. Only one thread should access this map at a time.
  */
class SequenceMap[K, V] {

  private case class SequenceEntry(valueToIndex: mutable.Map[V, Int], indexToValue: mutable.Map[Int, V]) {

    def insert(value: V): Int = {
      if (valueToIndex.contains(value)) return valueToIndex(value)

      val newIndex = indexToValue.size
      valueToIndex.update(value, newIndex)
      indexToValue.update(newIndex, value)
      newIndex
    }

    def nextIndex: Int = indexToValue.size

    def contains(value: V): Boolean = valueToIndex.contains(value)

  }

  private val map: mutable.Map[K, SequenceEntry] = mutable.Map.empty

  def insert(key: K, value: V): Int = {
    require(key != null, "Key cannot be null")
    require(value != null, "Value cannot be null")

    map.get(key) match {
      case Some(entry) => entry.insert(value)
      case None =>
        val entry = SequenceEntry(mutable.Map.empty, mutable.Map.empty)
        val newIndex = entry.insert(value)
        map.update(key, entry)
        newIndex
    }
  }

  def contains(key: K, value: V): Boolean = {
    require(key != null, "Key cannot be null")
    require(value != null, "Value cannot be null")

    map.get(key) match {
      case Some(entry) => entry.contains(value)
      case None        => false
    }
  }

  def potentialIndex(key: K, value: V): Int = {
    require(key != null, "Key cannot be null")
    require(value != null, "Value cannot be null")

    map.get(key) match {
      case Some(entry) => entry.valueToIndex.getOrElse(value, entry.nextIndex)
      case None        => 0
    }
  }

  def get(key: K, index: Int): V = {
    require(key != null, "Key cannot be null")
    require(index >= 0, "Index cannot be negative")
    require(map.contains(key), s"Key $key not found")

    val indexToValue = map(key).indexToValue
    require(indexToValue.contains(index), s"Index $index not found")

    indexToValue(index)
  }
}
