package ai.chronon.api.planner

import ai.chronon.api.PartitionRange
import ai.chronon.api

trait BatchRunContext {
  def partitionSpecWithColumn: PartitionSpecWithColumn
}
// run context in our case will be tableUtils
trait NodeRunner[Conf] {
  def run(metadata: api.MetaData, conf: Conf, range: PartitionRange, batchContext: BatchRunContext)
}

object LineageOfflineRunner {
  def readFiles(folderPath: String): Seq[Any] = {
    // read files from folder using metadata
    Seq.empty
  }
}
