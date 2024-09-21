package ai.chronon.overwatch

import ai.chronon.api.DataType


/**
 * Batch - Numeric metric computation
 *
 * 1. Read schema of the table
 * 2. Identify categorical, continuous, textual and
 * 2. For each tile, aggregate into KLL sketch tiles of 5 minutes
 * 3.
 */

// interface for metrics computation
trait WarehouseInterface {
  def getSchema(table: String): DataType

}


