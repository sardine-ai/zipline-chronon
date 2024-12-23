package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.Api
import ai.chronon.online.ExternalSourceRegistry
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import ai.chronon.online.LoggableResponse
import ai.chronon.online.Serde
import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider

class GcpApiImpl(conf: Map[String, String]) extends Api(conf) {

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): Serde =
    new AvroStreamDecoder(groupByServingInfoParsed.streamChrononSchema)

  override def genKvStore: KVStore = {
    val projectId = sys.env
      .getOrElse("GCP_PROJECT_ID", throw new IllegalArgumentException("GCP_PROJECT_ID environment variable not set"))
    val instanceId = sys.env
      .getOrElse("GCP_INSTANCE_ID", throw new IllegalArgumentException("GCP_INSTANCE_ID environment variable not set"))

    // Create settings builder based on whether we're in emulator mode (e.g. docker) or not
    val (dataSettingsBuilder, adminSettingsBuilder, maybeBQClient) = sys.env.get("BIGTABLE_EMULATOR_HOST") match {
      case Some(emulatorHostPort) =>
        val (emulatorHost, emulatorPort) = (emulatorHostPort.split(":")(0), emulatorHostPort.split(":")(1).toInt)
        val dataSettingsBuilder =
          BigtableDataSettings
            .newBuilderForEmulator(emulatorHost, emulatorPort)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .setMetricsProvider(NoopMetricsProvider.INSTANCE) // opt out of metrics in emulator
        val adminSettingsBuilder =
          BigtableTableAdminSettings
            .newBuilderForEmulator(emulatorHost, emulatorPort)
            .setCredentialsProvider(NoCredentialsProvider.create())

        (dataSettingsBuilder, adminSettingsBuilder, None)
      case None =>
        val dataSettingsBuilder = BigtableDataSettings.newBuilder()
        val adminSettingsBuilder = BigtableTableAdminSettings.newBuilder()
        val bigQueryClient = Some(BigQueryOptions.getDefaultInstance.getService)
        (dataSettingsBuilder, adminSettingsBuilder, bigQueryClient)
    }

    val dataSettings = dataSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
    val adminSettings = adminSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
    val dataClient = BigtableDataClient.create(dataSettings)
    val adminClient = BigtableTableAdminClient.create(adminSettings)

    new BigTableKVStoreImpl(dataClient, adminClient, maybeBQClient)
  }

  // TODO: Load from user jar.
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()
  override def externalRegistry: ExternalSourceRegistry = registry

  //TODO - Implement this
  override def logResponse(resp: LoggableResponse): Unit = {}
}
