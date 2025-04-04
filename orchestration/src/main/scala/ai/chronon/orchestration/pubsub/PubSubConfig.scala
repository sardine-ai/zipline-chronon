package ai.chronon.orchestration.pubsub

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import io.grpc.ManagedChannelBuilder

/** Base configuration trait for Pub/Sub clients.
  *
  * This trait defines the common interface for all Pub/Sub configurations,
  * regardless of the underlying implementation. It provides a basis for
  * configuration that can be shared across different Pub/Sub components
  * (publishers, subscribers, admin clients).
  *
  * The trait requires a unique identifier for each configuration, which enables:
  * - Caching of client instances with the same configuration
  * - Distinguishing between different configurations in logs and metrics
  * - Resource management based on configuration identity
  */
trait PubSubConfig {

  /** Returns a unique identifier for this configuration.
    *
    * This ID should uniquely identify the configuration settings, allowing
    * components to recognize equivalent configurations.
    *
    * @return A string that uniquely identifies this configuration
    */
  def id: String
}

/** Configuration for Google Cloud Pub/Sub clients.
  *
  * This class contains all the necessary configuration parameters for connecting
  * to Google Cloud Pub/Sub services, including:
  * - Project identification
  * - Transport channel configuration
  * - Authentication credentials
  *
  * The configuration can be used for both production environments (with default
  * credentials) and development/test environments (with emulator settings).
  *
  * @param projectId The Google Cloud project ID
  * @param channelProvider Optional custom transport channel provider (for emulator or testing)
  * @param credentialsProvider Optional custom credentials provider (for emulator or testing)
  */
case class GcpPubSubConfig(
    projectId: String,
    channelProvider: Option[TransportChannelProvider] = None,
    credentialsProvider: Option[CredentialsProvider] = None
) extends PubSubConfig {

  /** Generates a unique identifier for this configuration.
    *
    * The ID combines the project ID with hashes of the channel and credentials
    * providers, ensuring configurations with different settings have different IDs.
    *
    * @return A string that uniquely identifies this configuration
    */
  override def id: String = s"${projectId}-${channelProvider.hashCode}-${credentialsProvider.hashCode}"
}

/** Companion object for GcpPubSubConfig with factory methods for common configurations.
  *
  * This object provides factory methods to create GcpPubSubConfig instances
  * preconfigured for common scenarios:
  * - Production use with default GCP credentials
  * - Local development with the Pub/Sub emulator
  */
object GcpPubSubConfig {

  /** Creates a configuration for production Google Cloud environments.
    *
    * This configuration uses the default GCP credentials available in the
    * environment (e.g., from service account files, application default credentials, etc.).
    * It's suitable for use in production Google Cloud environments like GCE, GKE, or Cloud Run.
    *
    * @param projectId The Google Cloud project ID
    * @return A configuration for production use
    */
  def forProduction(projectId: String): GcpPubSubConfig = {
    GcpPubSubConfig(projectId)
  }

  /** Creates a configuration for the local Pub/Sub emulator.
    *
    * This configuration sets up the necessary transport channel and credentials
    * to connect to a local Pub/Sub emulator. It's useful for development and testing
    * without needing actual GCP resources or credentials.
    *
    * @param projectId The project ID to use with the emulator (can be any string)
    * @param emulatorHost The emulator host:port address (default: localhost:8085)
    * @return Configuration optimized for emulator use
    */
  def forEmulator(projectId: String, emulatorHost: String = "localhost:8085"): GcpPubSubConfig = {
    // Create channel for emulator with plaintext (non-TLS) communication
    val channel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build()
    val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))

    // No credentials needed for emulator
    val credentialsProvider = NoCredentialsProvider.create()

    GcpPubSubConfig(
      projectId = projectId,
      channelProvider = Some(channelProvider),
      credentialsProvider = Some(credentialsProvider)
    )
  }
}
