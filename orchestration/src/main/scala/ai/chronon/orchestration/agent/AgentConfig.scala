package ai.chronon.orchestration.agent

/** Configuration for Agent components.
  *
  * TODO: To move these to a separate file which can be configured per client
  */
object AgentConfig {

  val orchestrationServiceHostname: String = "localhost"

  val orchestrationServicePort: Int = 3903

  val pollingIntervalMs: Long = 5 * 60 * 1000

  val statusReportingIntervalMs: Long = 5 * 60 * 1000

  val topicId: String = "test-topic"

  val gcpProjectId: String = "chronon-dev"

  val gcpRegion: String = "test-region"

  val gcpCustomerId: String = "test-customer"

  val bigTableInstanceId: String = "chronon-dev-bigtable"
}
