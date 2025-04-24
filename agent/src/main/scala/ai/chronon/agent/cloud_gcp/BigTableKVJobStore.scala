package ai.chronon.agent.cloud_gcp

import ai.chronon.agent.{JobStore, KVJobStore}
import ai.chronon.integrations.cloud_gcp.BigTableKVStoreImpl
import com.google.cloud.bigtable.admin.v2.{BigtableTableAdminClient, BigtableTableAdminSettings}
import com.google.cloud.bigtable.data.v2.{BigtableDataClient, BigtableDataSettings}

object BigTableKVJobStore {
  def apply(projectId: String, bigTableInstanceId: String): JobStore = {
    // Create BigTable clients
    val dataSettings = BigtableDataSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(bigTableInstanceId)
      .build()

    val adminSettings = BigtableTableAdminSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(bigTableInstanceId)
      .build()

    val bigtableDataClient = BigtableDataClient.create(dataSettings)
    val bigtableAdminClient = BigtableTableAdminClient.create(adminSettings)

    val bigTableKVStore = new BigTableKVStoreImpl(
      bigtableDataClient,
      Some(bigtableAdminClient)
    )

    new KVJobStore(bigTableKVStore)
  }
}
