package ai.chronon.spark.stats.drift
import ai.chronon.api.Constants
import ai.chronon.online.Api
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SummaryUploader(summaryDF: DataFrame, api: Api, putsPerRequest: Int = 100) extends Serializable {
  private val statsTableName = Constants.DriftStatsTable

  def run(): Unit = {
    // Validate schema
    val requiredColumns = Seq("keyBytes", "valueBytes", "timestamp")
    val missingColumns = requiredColumns.filterNot(summaryDF.columns.contains)
    require(missingColumns.isEmpty, s"Missing required columns: ${missingColumns.mkString(", ")}")

    summaryDF.rdd.foreachPartition(rows => {
      val kvStore: KVStore = api.genKvStore

      val putRequests = new scala.collection.mutable.ArrayBuffer[PutRequest]
      for (row <- rows) {
        putRequests += PutRequest(
          Option(row.getAs[Array[Byte]]("keyBytes")).getOrElse(Array.empty[Byte]),
          Option(row.getAs[Array[Byte]]("valueBytes")).getOrElse(Array.empty[Byte]),
          statsTableName,
          Option(row.getAs[Long]("timestamp"))
        )
      }

      val futureResults = putRequests.grouped(putsPerRequest).map { batch =>
        kvStore
          .multiPut(batch.toList)
          .map { result =>
            if (!result.forall(identity)) {
              throw new RuntimeException(s"Failed to put ${result.count(!_)} records")
            }
          }
          .recover {
            case e =>
              throw new RuntimeException(s"Failed to put batch: ${e.getMessage}", e)
          }
      }

      val aggregatedFuture = Future.sequence(futureResults.toSeq)
      aggregatedFuture.onComplete {
        case scala.util.Success(_) => // All operations completed successfully
        case scala.util.Failure(e: IllegalArgumentException) =>
          throw new IllegalArgumentException(s"Invalid request data: ${e.getMessage}", e)
        case scala.util.Failure(e: java.io.IOException) =>
          throw new RuntimeException(s"KVStore I/O error: ${e.getMessage}", e)
        case scala.util.Failure(e) =>
          throw new RuntimeException(s"Failed to upload summary statistics: ${e.getMessage}", e)
      }
    })
  }
}
