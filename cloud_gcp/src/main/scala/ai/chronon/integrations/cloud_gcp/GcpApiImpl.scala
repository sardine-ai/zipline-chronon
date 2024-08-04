package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.{Api, ExternalSourceRegistry, GroupByServingInfoParsed, KVStore, LoggableResponse, Serde}

class GcpApiImpl(projectId: String, instanceId: String, conf: Map[String, String]) extends Api(conf) {

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): Serde =
    new AvroStreamDecoder(groupByServingInfoParsed.streamChrononSchema)

  override def genKvStore: KVStore = new BigTableKVStoreImpl(projectId, instanceId)

  // TODO: Load from user jar.
  override def externalRegistry: ExternalSourceRegistry = ???

  /** logged responses should be made available to an offline log table in Hive
   * with columns
   * key_bytes, value_bytes, ts_millis, join_name, schema_hash and ds (date string)
   * partitioned by `join_name` and `ds`
   * Note the camel case to snake case conversion: Hive doesn't like camel case.
   * The key bytes and value bytes will be transformed by chronon to human readable columns for each join.
   * <team_namespace>.<join_name>_logged
   * To measure consistency - a Side-by-Side comparison table will be created at
   * <team_namespace>.<join_name>_comparison
   * Consistency summary will be available in
   * <logTable>_consistency_summary
   */
  override def logResponse(resp: LoggableResponse): Unit = ???
}
