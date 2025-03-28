package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.Api
import ai.chronon.online.ExternalSourceRegistry
import ai.chronon.online.FlagStore
import ai.chronon.online.FlagStoreConstants
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import ai.chronon.online.LoggableResponse
import ai.chronon.online.Serde
import ai.chronon.online.serde.AvroSerde
import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider

import java.util

class GcpApiImpl(conf: Map[String, String]) extends Api(conf) {

  // For now we have a flag store that relies on some hardcoded values. Over time we can replace this with something
  // more sophisticated (e.g. service / teams.json based flags)
  val tilingEnabledFlagStore: FlagStore = (flagName: String, _: util.Map[String, String]) => {
    if (flagName == FlagStoreConstants.TILING_ENABLED) {
      true
    } else {
      false
    }
  }

  // We set the flag store to always return true for tiling enabled
  setFlagStore(tilingEnabledFlagStore)

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): Serde =
    new AvroSerde(groupByServingInfoParsed.streamChrononSchema)

  override def genKvStore: KVStore = {

    val projectId = sys.env
      .get("GCP_PROJECT_ID")
      .orElse(conf.get("GCP_PROJECT_ID"))
      .getOrElse(throw new IllegalArgumentException("GCP_PROJECT_ID environment variable not set"))

    val instanceId = sys.env
      .get("GCP_BIGTABLE_INSTANCE_ID")
      .orElse(conf.get("GCP_BIGTABLE_INSTANCE_ID"))
      .getOrElse(throw new IllegalArgumentException("GCP_BIGTABLE_INSTANCE_ID environment variable not set"))

    val maybeAppProfileId = sys.env
      .get("GCP_BIGTABLE_APP_PROFILE_ID")
      .orElse(conf.get("GCP_BIGTABLE_APP_PROFILE_ID"))

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

    val dataSettingsBuilderWithProfileId =
      maybeAppProfileId.map(profileId => dataSettingsBuilder.setAppProfileId(profileId)).getOrElse(dataSettingsBuilder)
    val dataSettings = dataSettingsBuilderWithProfileId.setProjectId(projectId).setInstanceId(instanceId).build()
    val dataClient = BigtableDataClient.create(dataSettings)

    val adminSettings = adminSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
    val adminClient = BigtableTableAdminClient.create(adminSettings)

    new BigTableKVStoreImpl(dataClient, adminClient, maybeBQClient)
  }

  // TODO: Load from user jar.
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  //TODO - Implement this
  override def logResponse(resp: LoggableResponse): Unit = {}
}
