package ai.chronon.flink.types

import java.util
import java.util.Objects

// This file contains PoJo classes that are persisted while taking checkpoints in Chronon's Flink jobs. This falls primarily
// in two buckets - tiled state and KV store incoming / outgoing records. The classes used in these cases need to allow for state
// schema evolution (https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/)
// This allows us to add / remove fields without requiring us to migrate the state using dual write / read patterns.

/**
  * Combines the IR (intermediate result) with the timestamp of the event being processed.
  * We need the timestamp of the event processed so we can calculate processing lag down the line.
  *
  * Example: for a GroupBy with 2 windows, we'd have TimestampedTile( [IR for window 1, IR for window 2], timestamp ).
  *
  * @param ir the array of partial aggregates
  * @param latestTsMillis timestamp of the current event being processed
  */
class TimestampedIR(var ir: Array[Any], var latestTsMillis: Option[Long]) {
  def this() = this(Array(), None)

  override def toString: String =
    s"TimestampedIR(ir=${ir.mkString(", ")}, latestTsMillis=$latestTsMillis)"

  override def hashCode(): Int =
    Objects.hash(ir.deep, latestTsMillis)

  override def equals(other: Any): Boolean =
    other match {
      case e: TimestampedIR =>
        util.Arrays.deepEquals(ir.asInstanceOf[Array[AnyRef]],
                               e.ir.asInstanceOf[Array[AnyRef]]) && latestTsMillis == e.latestTsMillis
      case _ => false
    }
}

/**
  * Combines the entity keys, the encoded IR (intermediate result), and the timestamp of the event being processed.
  *
  * We need the timestamp of the event processed so we can calculate processing lag down the line.
  *
  * @param keys the GroupBy entity keys
  * @param tileBytes encoded tile IR
  * @param latestTsMillis timestamp of the current event being processed
  *
  * Changed keys type to Seq[Any] instead of List[Any] otherwise we are running into accessing head of null list
  * runtime error for tests which is very weird and was hard to debug the root cause.
  */
class TimestampedTile(var keys: Seq[Any], var tileBytes: Array[Byte], var latestTsMillis: Long) {
  def this() = this(List(), Array(), 0L)

  override def toString: String =
    s"TimestampedTile(keys=${keys.mkString(", ")}, tileBytes=${java.util.Base64.getEncoder
      .encodeToString(tileBytes)}, latestTsMillis=$latestTsMillis)"

  override def hashCode(): Int =
    Objects.hash(keys.toArray.deep, tileBytes, latestTsMillis.asInstanceOf[java.lang.Long])

  override def equals(other: Any): Boolean =
    other match {
      case e: TimestampedTile =>
        util.Arrays.deepEquals(keys.toArray.asInstanceOf[Array[AnyRef]], e.keys.toArray.asInstanceOf[Array[AnyRef]]) &&
          util.Arrays.equals(tileBytes, e.tileBytes) &&
          latestTsMillis == e.latestTsMillis
      case _ => false
    }
}

/**
  * Output emitted by the AvroCodecFn operator. This is fed into the Async KV store writer and objects of this type are persisted
  * while taking checkpoints.
  */
class AvroCodecOutput(var keyBytes: Array[Byte], var valueBytes: Array[Byte], var dataset: String, var tsMillis: Long) {
  def this() = this(Array(), Array(), "", 0L)

  override def hashCode(): Int =
    Objects.hash(
      keyBytes,
      valueBytes,
      dataset,
      tsMillis.asInstanceOf[java.lang.Long]
    )

  override def equals(other: Any): Boolean =
    other match {
      case o: AvroCodecOutput =>
        util.Arrays.equals(keyBytes, o.keyBytes) &&
          util.Arrays.equals(valueBytes, o.valueBytes) &&
          dataset == o.dataset &&
          tsMillis == o.tsMillis
      case _ => false
    }
}

/**
  * Output records emitted by the AsyncKVStoreWriter. Objects of this type are persisted while taking checkpoints.
  */
class WriteResponse(var keyBytes: Array[Byte],
                    var valueBytes: Array[Byte],
                    var dataset: String,
                    var tsMillis: Long,
                    var status: Boolean) {
  def this() = this(Array(), Array(), "", 0L, false)

  override def hashCode(): Int =
    Objects.hash(keyBytes,
                 valueBytes,
                 dataset,
                 tsMillis.asInstanceOf[java.lang.Long],
                 status.asInstanceOf[java.lang.Boolean])

  override def equals(other: Any): Boolean =
    other match {
      case o: WriteResponse =>
        util.Arrays.equals(keyBytes, o.keyBytes) &&
          util.Arrays.equals(valueBytes, o.valueBytes) &&
          dataset == o.dataset &&
          tsMillis == o.tsMillis &&
          status == o.status
      case _ => false
    }
}
