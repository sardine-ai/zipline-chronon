package ai.chronon.spark.stats.drift

import ai.chronon.api.ColorPrinter.ColorString
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{Constants, DriftSpec, DriftStatsServingInfo, MetaData, ThriftJsonCodec}
import ai.chronon.online.{PartitionRange, SparkConversions}
import ai.chronon.spark.{TableUtils, TimedKvRdd}
import org.apache.spark.rdd.RDD
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.drift.SummaryUploader.buildServingInfo
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

class SummaryUploader(inputDf: DataFrame, driftSpec: DriftSpec, ds: String)(implicit val tu: TableUtils) {

  private[spark] def computeUploadDf: DataFrame = {
    // Expect a df of: _tile, ds, slice, feature, metrics
    assert(inputDf.columns.contains(Constants.TileColumn),
           s"Expect the drift stats pivot table to include the ${Constants.TileColumn} field")
    val tsIdx = inputDf.schema.fieldIndex(Constants.TileColumn)

    val keyCols = Seq(Constants.SliceColumn, Constants.FeatureColumn)
    keyCols.foreach(k =>
      assert(inputDf.columns.contains(k), s"Expect the drift stats pivot table to include the $k field"))

    val keyIdxes = keyCols.map(k => inputDf.schema.fieldIndex(k))

    assert(inputDf.columns.contains(Constants.MetricsColumn),
           s"Expect the drift stats pivot table to include the ${Constants.MetricsColumn} field")
    val metricsIdx = inputDf.schema.fieldIndex(Constants.MetricsColumn)

    val dataRDD: RDD[(Array[Any], Array[Any], Long)] = inputDf.rdd.map { row =>
      val keyArray: Array[Any] = keyIdxes.map(i => row.get(i)).toArray
      val tileTs: Long = row.getDouble(tsIdx).toLong
      val valueArray: Array[Any] = Array(row.get(metricsIdx))
      (keyArray, valueArray, tileTs)
    }

    implicit val sparkSession = inputDf.sparkSession
    val keySchema = SparkConversions.fromChrononSchema(Constants.DriftStatsKeySchema)
    val valueSchema = StructType(Seq(inputDf.schema.fields(metricsIdx)))
    val timedKvRdd = TimedKvRdd(dataRDD, keySchema, valueSchema)
    val uploadDf = timedKvRdd.toAvroDf.withTimeBasedColumn(tu.partitionColumn)

    // we write one meta row for the last day partition rather than one meta row per
    // missing day partition (as we only care about the latest metadata)
    val servingInfo = buildServingInfo(driftSpec, keySchema, valueSchema)
    val metaRows = Seq(
      Row(
        Constants.DriftStatsServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(servingInfo).getBytes(Constants.UTF8),
        Constants.DriftStatsServingInfoKey,
        ThriftJsonCodec.toJsonStr(servingInfo),
        tu.partitionSpec.epochMillis(ds), // set ts == ds
        ds
      ))

    val metaRdd = tu.sparkSession.sparkContext.parallelize(metaRows)
    val metaDf = tu.sparkSession.createDataFrame(metaRdd, uploadDf.schema)
    uploadDf
      .union(metaDf)
  }
}

object SummaryUploader {

  def run(metadata: MetaData, driftSpec: DriftSpec, ds: String)(implicit tu: TableUtils): Unit = {
    // TODO: replace with the pivoted summary table!!
    val inputTable = metadata.summaryTable
    val uploadTable = metadata.summaryUploadTable

    // TODO: refactor as this code is identical with others

    // check existing partitions
    val inputPartitions = tu.partitions(inputTable)
    val existingUploadPartitions = tu.partitions(uploadTable)
    val missingUploadPartitions = (inputPartitions.toSet -- existingUploadPartitions.toSet).filter(_ <= ds)

    println(s"""
               |Summary table to upload: $inputTable, ${tu.partitionRange(inputTable)}
               |KV store ready data will be written to table: $uploadTable, ${tu.partitionRange(uploadTable)}
               |Missing Partitions: [${missingUploadPartitions.toSeq.sorted.mkString(", ")}]
               |""".stripMargin.yellow)

    if (missingUploadPartitions.isEmpty) {
      println("""Nothing left to upload. Exiting..""".red)
    } else {
      val minMissing = missingUploadPartitions.min
      val maxMissing = missingUploadPartitions.max
      val missingRange = PartitionRange(minMissing, maxMissing)(tu.partitionSpec)
      val inputDf = tu.loadTable(inputTable).filter(missingRange.whereClauses(tu.partitionColumn).mkString(" AND "))

      val uploader = new SummaryUploader(inputDf, driftSpec, ds)
      val uploadDf = uploader.computeUploadDf
      uploadDf.save(uploadTable)
    }
  }

  def buildServingInfo(driftSpec: DriftSpec, keySchema: StructType, valueSchema: StructType): DriftStatsServingInfo = {
    val statsServingInfo = new DriftStatsServingInfo()
    statsServingInfo.setSpec(driftSpec)
    statsServingInfo.setKeyAvroSchema(keySchema.toAvroSchema("Key").toString(true))
    statsServingInfo.setValueAvroSchema(valueSchema.toAvroSchema("Value").toString(true))
    statsServingInfo
  }
}
