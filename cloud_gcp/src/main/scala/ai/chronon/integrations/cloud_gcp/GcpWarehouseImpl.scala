package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.StructType
import ai.chronon.online.connectors
import ai.chronon.online.connectors.{Catalog, DataSpec, Topic, Warehouse}
import com.google.cloud.bigquery._
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, TopicAdminClient}
import com.google.pubsub.v1.{ProjectSubscriptionName, PushConfig, Subscription, TopicName}
import org.apache.flink.streaming.api.scala.DataStream
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter


class PubSubMessageBusImpl(projectId: String, catalog: Catalog) extends connectors.MessageBus(catalog) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  override protected def createTopicInternal(topic: Topic, spec: DataSpec): Unit = {
    try {
      val topicName = TopicName.of(projectId, topic.name)
      TopicAdminClient.create().createTopic(topicName)
      logger.info(s"Topic ${topicName.getTopic} created.")
    } catch {
      case e: Exception => logger.error(s"Error creating topic: ${e.getMessage}")
    }
  }

  override protected def writeBytes(topic: Topic, data: Array[Byte], key: Array[Byte]): Unit = ???

  override protected def readBytes(topic: Topic): DataStream[Array[Byte]] = ???
}

class GcpWarehouseImpl(projectId: String, catalog: Catalog) extends Warehouse(catalog) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  private val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService
  override def createTopic(topic: Topic, spec: DataSpec): Unit = {
    try {
      val topicName = TopicName.of(projectId, topicName)
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
    val schema = BigQuerySchemaConverter.convertToBigQuerySchema(spec.schema)

    val tableId = TableId.of(projectId, table.databaseName, table.tableName)

    // Create the table definition
    val tableDefinitionBuilder = StandardTableDefinition
      .newBuilder()
      .setSchema(schema)

    // Add partitioning if specified
    if (spec.partitionColumns.nonEmpty) {
      val partitioning = spec.partitionColumns.map { columnName =>
        schema.getFields.asScala.find(_.getName == columnName) match {
          case Some(field) => createTimePartitioning(field, spec.retentionDays)
          case None        => throw new IllegalArgumentException(s"Partition column $columnName not found in schema")
        }
      }.head // BigQuery supports only one partition column, so we take the first one

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
      spec.retentionDays.foreach { days =>
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



  override def appendQueryOutput(query: String, table: connectors.Table): Unit = ???
}