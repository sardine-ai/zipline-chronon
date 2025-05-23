package ai.chronon.spark.ingest
import org.apache.spark.sql.SparkSession
import ai.chronon.api.PartitionRange

abstract class DataImport {

  def sync(sourceTableName: String, destinationTableName: String, partitionRange: PartitionRange)(implicit
      sparkSession: SparkSession): Unit

}
