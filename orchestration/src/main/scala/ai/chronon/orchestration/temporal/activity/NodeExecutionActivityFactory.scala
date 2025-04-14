package ai.chronon.orchestration.temporal.activity

import ai.chronon.orchestration.persistence.NodeDao
import ai.chronon.orchestration.pubsub.{GcpPubSubConfig, PubSubManager, PubSubPublisher}
import ai.chronon.orchestration.temporal.workflow.WorkflowOperations
import io.temporal.client.WorkflowClient

/** Factory for creating NodeExecutionActivity implementations.
  *
  * This factory follows the Factory pattern to create fully configured NodeExecutionActivity
  * instances with appropriate dependencies. It provides multiple creation methods to support:
  *
  * 1. Different environments (production, local emulation)
  * 2. Multiple configuration sources (environment variables, explicit parameters)
  * 3. Custom dependency injection
  *
  * The factory handles:
  * - Setting up PubSub connections appropriately for different environments
  * - Creating WorkflowOperations instances
  * - Wiring dependencies together
  * - Providing sensible defaults
  *
  * Using this factory ensures consistent activity creation throughout the application
  * and simplifies testing by allowing dependency substitution.
  */
object NodeExecutionActivityFactory {

  /** Creates a NodeExecutionActivity with configuration from environment variables.
    *
    * This method creates an activity implementation using environment variables for
    * PubSub configuration. It's convenient for production deployments where
    * configuration is provided through the environment.
    *
    * Required environment variables:
    * - GCP_PROJECT_ID: The Google Cloud project ID
    * - PUBSUB_TOPIC_ID: The PubSub topic for job submissions
    *
    * @param workflowClient The Temporal workflow client
    * @param nodeDao The data access object for node persistence
    * @return A NodeExecutionActivity configured from environment variables
    */
  def create(workflowClient: WorkflowClient, nodeDao: NodeDao): NodeExecutionActivity = {
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

    create(workflowClient, nodeDao, projectId, topicId)
  }

  /** Creates a NodeExecutionActivity with a custom PubSubManager.
    *
    * @param workflowClient The Temporal workflow client
    * @param nodeDao The data access object for node persistence
    * @param pubSubManager A custom PubSub Manager
    * @param topicId The PubSub topic ID for job submissions
    * @return A NodeExecutionActivity with the specified PubSubManager
    */
  def create(
      workflowClient: WorkflowClient,
      nodeDao: NodeDao,
      pubSubManager: PubSubManager,
      topicId: String
  ): NodeExecutionActivity = {
    val publisher = pubSubManager.getOrCreatePublisher(topicId)

    val workflowOps = new WorkflowOperations(workflowClient)
    new NodeExecutionActivityImpl(workflowOps, nodeDao, publisher)
  }

  /** Creates a NodeExecutionActivity with explicitly provided configuration.
    * It automatically detects whether to use the PubSub emulator based on environment variables.
    *
    * @param workflowClient The Temporal workflow client
    * @param nodeDao The data access object for node persistence
    * @param projectId The GCP project ID for PubSub
    * @param topicId The PubSub topic ID for job submissions
    * @return A fully configured NodeExecutionActivity implementation
    */
  def create(workflowClient: WorkflowClient,
             nodeDao: NodeDao,
             projectId: String,
             topicId: String): NodeExecutionActivity = {
    // Create PubSub configuration based on environment
    val manager = sys.env.get("PUBSUB_EMULATOR_HOST") match {
      case Some(emulatorHost) =>
        // Use emulator configuration if PUBSUB_EMULATOR_HOST is set
        PubSubManager.forEmulator(projectId, emulatorHost)
      case None =>
        // Use default configuration for production
        PubSubManager.forProduction(projectId)
    }

    create(workflowClient, nodeDao, manager, topicId)
  }

  /** Creates a NodeExecutionActivity with a custom PubSub configuration.
    *
    * @param workflowClient The Temporal workflow client
    * @param nodeDao The data access object for node persistence
    * @param config A custom GCP PubSub configuration
    * @param topicId The PubSub topic ID for job submissions
    * @return A NodeExecutionActivity with the specified PubSub configuration
    */
  def create(
      workflowClient: WorkflowClient,
      nodeDao: NodeDao,
      config: GcpPubSubConfig,
      topicId: String
  ): NodeExecutionActivity = {
    val manager = PubSubManager(config)
    create(workflowClient, nodeDao, manager, topicId)
  }

  /** Creates a NodeExecutionActivity with a pre-configured PubSub publisher.
    *
    * @param workflowClient The Temporal workflow client
    * @param nodeDao The data access object for node persistence
    * @param pubSubPublisher A pre-configured PubSub publisher
    * @return A NodeExecutionActivity using the provided publisher
    */
  def create(workflowClient: WorkflowClient,
             nodeDao: NodeDao,
             pubSubPublisher: PubSubPublisher): NodeExecutionActivity = {
    val workflowOps = new WorkflowOperations(workflowClient)
    new NodeExecutionActivityImpl(workflowOps, nodeDao, pubSubPublisher)
  }
}
