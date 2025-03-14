package ai.chronon.integrations.cloud_gcp
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import org.apache.spark.sql.SparkSession
import com.google.cloud.bigquery.TableId

object SparkBQUtils {

  def toTableId(tableName: String)(implicit spark: SparkSession): TableId = {
    val parseIdentifier = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val shadedTid = BigQueryUtil.parseTableId(parseIdentifier.mkString("."))
    scala
      .Option(shadedTid.getProject)
      .map(TableId.of(_, shadedTid.getDataset, shadedTid.getTable))
      .getOrElse(TableId.of(shadedTid.getDataset, shadedTid.getTable))
  }

}
