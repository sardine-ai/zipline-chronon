package ai.chronon.orchestration.utils

import ai.chronon.orchestration.pubsub.GcpPubSubConfig
import com.google.cloud.pubsub.v1.{
  SubscriptionAdminClient,
  SubscriptionAdminSettings,
  TopicAdminClient,
  TopicAdminSettings
}
import org.slf4j.LoggerFactory

/** Utility class for creating GCP PubSub admin clients
  */
object GcpPubSubAdminUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Create a topic admin client for Google Cloud PubSub
    * @param config The GCP PubSub configuration
    * @return A TopicAdminClient configured with the provided settings
    */
  def createTopicAdminClient(config: GcpPubSubConfig): TopicAdminClient = {
    val topicAdminSettingsBuilder = TopicAdminSettings.newBuilder()

    // Add channel provider if specified
    config.channelProvider.foreach { provider =>
      logger.info("Using custom channel provider for TopicAdminClient")
      topicAdminSettingsBuilder.setTransportChannelProvider(provider)
    }

    // Add credentials provider if specified
    config.credentialsProvider.foreach { provider =>
      logger.info("Using custom credentials provider for TopicAdminClient")
      topicAdminSettingsBuilder.setCredentialsProvider(provider)
    }

    TopicAdminClient.create(topicAdminSettingsBuilder.build())
  }

  /** Create a subscription admin client for Google Cloud PubSub
    * @param config The GCP PubSub configuration
    * @return A SubscriptionAdminClient configured with the provided settings
    */
  def createSubscriptionAdminClient(config: GcpPubSubConfig): SubscriptionAdminClient = {
    val subscriptionAdminSettingsBuilder = SubscriptionAdminSettings.newBuilder()

    // Add channel provider if specified
    config.channelProvider.foreach { provider =>
      logger.info("Using custom channel provider for SubscriptionAdminClient")
      subscriptionAdminSettingsBuilder.setTransportChannelProvider(provider)
    }

    // Add credentials provider if specified
    config.credentialsProvider.foreach { provider =>
      logger.info("Using custom credentials provider for SubscriptionAdminClient")
      subscriptionAdminSettingsBuilder.setCredentialsProvider(provider)
    }

    SubscriptionAdminClient.create(subscriptionAdminSettingsBuilder.build())
  }
}
