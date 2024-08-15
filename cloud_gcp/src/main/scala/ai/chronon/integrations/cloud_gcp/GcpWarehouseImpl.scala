package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.{DataSpec, DataType}
import ai.chronon.online.connectors
import ai.chronon.online.connectors.{Catalog, Topic, Warehouse}
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryException,
  BigQueryOptions,
  DatasetInfo,
  Field,
  JobId,
  JobInfo,
  QueryJobConfiguration,
  StandardSQLTypeName,
  StandardTableDefinition,
  TableId,
  TableInfo,
  TimePartitioning
}
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, TopicAdminClient}
import com.google.pubsub.v1.{ProjectSubscriptionName, PushConfig, Subscription, TopicName}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class GcpWarehouseImpl(projectId: String, catalog: Catalog) extends Warehouse(catalog) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  private val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
  def createTopic(topic: Topic, spec: DataSpec): Unit = {
    try {
      val topicName = TopicName.of(projectId, topic.name)
      TopicAdminClient.create().createTopic(topicName)
      logger.info(s"Topic ${topicName.getTopic} created.")
    } catch {
      case e: Exception => logger.error(s"Error creating topic: ${e.getMessage}")
    }
  }

  override def createDatabase(databaseName: String): Unit = {
    val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val dataset = bigquery.create(DatasetInfo.newBuilder(databaseName).build())
    logger.info(s"Dataset ${dataset.getDatasetId.getDataset} created.")
  }

  override def createTableInternal(table: connectors.Table, spec: DataSpec): Unit = {
    val schema = BigQuerySchemaConverter.convertToBigQuerySchema(DataType.fromTDataType(spec.schema))

    val tableId = TableId.of(projectId, table.databaseName, table.tableName)

    // Create the table definition
    val tableDefinitionBuilder = StandardTableDefinition
      .newBuilder()
      .setSchema(schema)

    val retentionDays = if (spec.isSetRetentionDays) { Option(spec.getRetentionDays) }
    else { None }
    Option(spec.partitionColumns).foreach { cols =>
      assert(!cols.isEmpty, "Partition columns must be a non-empty list or null")
      assert(cols.size == 1, "BigQuery supports only one partition column")
      val col = cols.get(0)
      val field = schema.getFields.asScala.find(_.getName == col)
      lazy val fieldNames = schema.getFields.asScala.map(_.getName).mkString(", ")
      assert(field.nonEmpty, s"Partition column $col not found in schema. Available columns: $fieldNames")
      val partitioning = createTimePartitioning(field.get, return
      )
      tableDefinitionBuilder.setTimePartitioning(partitioning)
    }

    val tableDefinition = tableDefinitionBuilder.build()

    // Create the table info
    val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build()

    try {
      // Create the table
      val createdTable = bigquery.create(tableInfo)
      println(s"Table ${createdTable.getTableId.getTable} created successfully.")

      // Log retention period if set
      retentionDays.foreach { days =>
        println(s"Table will expire after $days days.")
      }
    } catch {
      case e: BigQueryException =>
        println(s"Table creation failed: ${e.getMessage}")
        throw e
    }
  }

  private def createTimePartitioning(field: Field, retentionDays: Option[Int]): TimePartitioning = {
    val builder = field.getType.getStandardType match {
      case StandardSQLTypeName.DATE =>
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField(field.getName)
      case StandardSQLTypeName.TIMESTAMP =>
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField(field.getName)
      case _ =>
        throw new IllegalArgumentException(s"Partition column ${field.getName} must be of type DATE or TIMESTAMP")
    }
    retentionDays.foreach { days =>
      val expirationMs = TimeUnit.DAYS.toMillis(days.toLong)
      builder.setExpirationMs(expirationMs)
    }
    builder.build()
  }

  override def enableIngestion(topic: Topic, table: connectors.Table): Unit = {
    val databaseName = table.databaseName
    val tableName = table.tableName
    val topicName = topic.name
    try {
      val projectSubscriptionName = ProjectSubscriptionName.of(projectId, s"$databaseName.${tableName}_to_$topicName")
      val topic = TopicName.of(projectId, topicName)
      val pushConfig = PushConfig
        .newBuilder()
        .setPushEndpoint(
          s"https://bigquery.googleapis.com/bigquery/v2/projects/$projectId/datasets/$databaseName/tables/$tableName")
        .build()

      val subscription = Subscription
        .newBuilder()
        .setName(projectSubscriptionName.toString)
        .setTopic(topic.toString)
        .setPushConfig(pushConfig)
        .build()

      SubscriptionAdminClient.create().createSubscription(subscription)
      logger.info(s"Subscription ${subscription.getName} created.")
    } catch {
      case e: Exception => logger.error(s"Error creating subscription: ${e.getMessage}")
    }
  }

  // write the output of the bigquery query to the table
  // first time this is called table will be created
  // other times the data will be appended
  override def nativeQuery(query: String, table: connectors.Table): Unit = {
    val datasetId = table.databaseName
    val tableId = table.tableName

    // Step 1: Prepare the destination dataset
    val dataset = bigquery.getDataset(datasetId)
    if (dataset == null) {
      bigquery.create(DatasetInfo.newBuilder(datasetId).build())
    }

    // Step 2: Check if the destination table exists
    val existingTable = bigquery.getTable(datasetId, tableId)

    // Step 3: Prepare the query
    val finalQuery = if (existingTable != null) {
      // Construct INSERT statement for existing table
      val columnNames =
        existingTable.getDefinition[StandardTableDefinition].getSchema.getFields.asScala.map(_.getName).mkString(", ")
      s"""
      INSERT INTO `$projectId.$datasetId.$tableId` ($columnNames)
      $query
      """
    } else {
      // Construct CREATE TABLE AS SELECT (CTAS) for new table
      s"""
      CREATE TABLE `$projectId.$datasetId.$tableId` AS
      $query
      """
    }

    // Step 4: Execute the query
    val queryConfig = QueryJobConfiguration
      .newBuilder(finalQuery)
      .setUseLegacySql(false)
      .build()

    val jobId = JobId.of(java.util.UUID.randomUUID().toString)
    val job = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

    // Wait for the job to complete
    job.waitFor()

    if (job.getStatus.getError != null) {
      logger.error(s"Query failed: ${job.getStatus.getError}. Query: $finalQuery, Job ID: $jobId")
      throw new RuntimeException(s"Query failed: ${job.getStatus.getError}")
    }

    // Step 5: Get job statistics
    val queryStatistics = job.getStatistics.asInstanceOf[QueryStatistics]
    val rowsWritten = queryStatistics.getNumDmlAffectedRows
    val slotsSeconds = queryStatistics.getTotalSlotMs / 1000
    val timeTaken = (queryStatistics.getEndTime - queryStatistics.getStartTime) / 1000
    println(s"""
         |Wrote $rowsWritten rows to $datasetId.$tableId
         |  time taken: $timeTaken seconds
         |  slots consumed: $slotsSeconds slot seconds
         |  processed bytes: ${queryStatistics.getTotalBytesProcessed}
         |""".stripMargin)
  }
}
