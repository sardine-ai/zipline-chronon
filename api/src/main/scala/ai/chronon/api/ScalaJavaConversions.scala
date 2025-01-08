package ai.chronon.api

import scala.collection.parallel.ParSeq
import scala.jdk.CollectionConverters._

object ScalaJavaConversions {

  def toJava[T](list: Seq[T]): java.util.List[T] = {
    if (list == null) {
      null
    } else {
      list.asJava
    }
  }

  def toScala[T](list: java.util.List[T]): Seq[T] = {
    if (list == null) {
      null
    } else {
      list.asScala
    }
  }

  def toJava[K, V](map: Map[K, V]): java.util.Map[K, V] = {
    if (map == null) {
      null
    } else {
      map.asJava
    }
  }

  def toScala[K, V](map: java.util.Map[K, V]): Map[K, V] = {
    if (map == null) {
      null
    } else {
      map.asScala.toMap
    }
  }

  implicit class IteratorOps[T](iterator: java.util.Iterator[T]) {
    def toScala: Iterator[T] = {
      iterator.asScala
    }
  }
  implicit class JIteratorOps[T](iterator: Iterator[T]) {
    def toJava: java.util.Iterator[T] = {
      iterator.asJava
    }
  }
  implicit class ListOps[T](list: java.util.List[T]) {
    def toScala: List[T] = {
      if (list == null) {
        null
      } else {
        list.iterator().asScala.toList
      }
    }
  }
  implicit class JListOps[T](list: Seq[T]) {
    def toJava: java.util.List[T] = {
      if (list == null) {
        null
      } else {
        list.asJava
      }
    }
  }
  implicit class IterableOps[T](it: Iterable[T]) {
    def parallel: ParSeq[T] = {
      if (it == null) {
        null
      } else {
        it.toSeq.par
      }
    }
  }
  implicit class MapOps[K, V](map: java.util.Map[K, V]) {
    def toScala: Map[K, V] = {
      if (map == null) {
        null
      } else {
        map.asScala.toMap
      }
    }
  }
  implicit class JMapOps[K, V](map: Map[K, V]) {
    def toJava: java.util.Map[K, V] = {
      if (map == null) {
        null
      } else {
        map.asJava
      }
    }
  }
}
