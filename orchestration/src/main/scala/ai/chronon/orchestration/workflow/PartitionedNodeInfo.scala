package ai.chronon.orchestration.workflow

import ai.chronon.api.PartitionSpec
import ai.chronon.online.PartitionRange
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.mutable

class TableProducer[WorkDef](work: WorkDef, runContext: RunContext) {}

case class TableInfo(name: String,
                     partitionColumn: String,
                     partitionSpec: PartitionSpec,
                     existingPartitions: mutable.SortedSet[String]) {

  def missingPartitions(query: PartitionRange): Seq[String] = {
    query.partitions.filterNot(existingPartitions.contains)
  }
}


/**
 *
 * 1. cli: compute a map of file name to hash from a given local folder
 * 2. server: from the existing (file name -> file hash set) compute missing files and return
 * 3. cli:
 *       for each missing file, send (file_name, file_hash, column_lineage, file_contents) to server
 * 4: server: add to redis (file name + hash) -> file content
 * 5:
 *
 */