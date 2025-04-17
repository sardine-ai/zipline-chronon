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

  /** Dataset name for active job storage in KVStore */
  val kvStoreActiveJobsDataset: String = "agent_active_jobs"

  /** Dataset name for completed job storage in KVStore */
  val kvStoreCompletedJobsDataset: String = "agent_completed_jobs"

  /** Timeout for KVStore operations in milliseconds */
  val kvStoreTimeoutMs: Long = 10000

  /** GCP Project ID for BigTable */
  val gcpProjectId: String = "chronon-dev"

  /** BigTable instance ID */
  val bigTableInstanceId: String = "chronon-dev-bigtable"
}
