package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.pubsub.{GcpPubSubConfig, PubSubManager, PubSubPublisher}
import ai.chronon.orchestration.temporal.workflow.WorkflowOperationsImpl
import io.temporal.client.WorkflowClient

// Factory for creating activity implementations
object NodeExecutionActivityFactory {

  /** Create a NodeExecutionActivity with default configuration from environment variables.
    *
    * This method relies on environment variables for configuration:
    * - GCP_PROJECT_ID: The Google Cloud project ID (required)
    * - PUBSUB_TOPIC_ID: The PubSub topic for job submissions (required)
    *
    * @param workflowClient The Temporal workflow client
    * @return A NodeExecutionActivity configured from environment variables
    * @throws IllegalArgumentException if required environment variables are not set
    */
  def create(workflowClient: WorkflowClient): NodeExecutionActivity = {
    // Get environment variables with validation
    val projectId = sys.env.getOrElse(
      "GCP_PROJECT_ID",
      throw new IllegalArgumentException("Environment variable GCP_PROJECT_ID must be set"))

    val topicId = sys.env.getOrElse(
      "PUBSUB_TOPIC_ID",
      throw new IllegalArgumentException("Environment variable PUBSUB_TOPIC_ID must be set"))

    // Verify that they're not empty
    if (projectId.trim.isEmpty) {
      throw new IllegalArgumentException("Environment variable GCP_PROJECT_ID cannot be empty")
    }

    if (topicId.trim.isEmpty) {
      throw new IllegalArgumentException("Environment variable PUBSUB_TOPIC_ID cannot be empty")
    }

    create(workflowClient, projectId, topicId)
  }

  /** Create a NodeExecutionActivity with custom PubSub manager
    */
  def create(
      workflowClient: WorkflowClient,
      pubSubManager: PubSubManager,
      topicId: String
  ): NodeExecutionActivity = {
    val publisher = pubSubManager.getOrCreatePublisher(topicId)

    val workflowOps = new WorkflowOperationsImpl(workflowClient)
    new NodeExecutionActivityImpl(workflowOps, publisher)
  }

  /** Create a NodeExecutionActivity with explicit configuration
    */
  def create(workflowClient: WorkflowClient, projectId: String, topicId: String): NodeExecutionActivity = {
    // Create PubSub configuration based on environment
    val manager = sys.env.get("PUBSUB_EMULATOR_HOST") match {
      case Some(emulatorHost) =>
        // Use emulator configuration if PUBSUB_EMULATOR_HOST is set
        PubSubManager.forEmulator(projectId, emulatorHost)
      case None =>
        // Use default configuration for production
        PubSubManager.forProduction(projectId)
    }

    create(workflowClient, manager, topicId)
  }

  /** Create a NodeExecutionActivity with custom PubSub configuration
    */
  def create(
      workflowClient: WorkflowClient,
      config: GcpPubSubConfig,
      topicId: String
  ): NodeExecutionActivity = {
    val manager = PubSubManager(config)
    create(workflowClient, manager, topicId)
  }

  /** Create a NodeExecutionActivity with a pre-configured PubSub publisher
    */
  def create(workflowClient: WorkflowClient, pubSubPublisher: PubSubPublisher): NodeExecutionActivity = {
    val workflowOps = new WorkflowOperationsImpl(workflowClient)
    new NodeExecutionActivityImpl(workflowOps, pubSubPublisher)
  }
}
