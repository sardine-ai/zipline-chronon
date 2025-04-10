package ai.chronon.integrations.cloud_gcp

import ai.chronon.integrations.cloud_gcp.GcpApiImpl.executorProvider
import ai.chronon.online.Api
import ai.chronon.online.ExternalSourceRegistry
import ai.chronon.online.FlagStore
import ai.chronon.online.FlagStoreConstants
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import ai.chronon.online.LoggableResponse
import ai.chronon.online.serde.Serde
import ai.chronon.online.serde.AvroSerde
import com.google.api.gax.core.{InstantiatingExecutorProvider, NoCredentialsProvider}
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider

import java.util
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

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

    // We skip upload clients (e.g. admin client, bq client) in non-upload contexts (e.g. streaming & fetching)
    // This flag allows us to enable them in the upload contexts
    val enableUploadClients = sys.env
      .get("ENABLE_UPLOAD_CLIENTS")
      .orElse(conf.get("ENABLE_UPLOAD_CLIENTS"))
      .exists(_.toBoolean)

    // Create settings builder based on whether we're in emulator mode (e.g. docker) or not
    val (dataSettingsBuilder, maybeAdminSettingsBuilder, maybeBQClient) = sys.env.get("BIGTABLE_EMULATOR_HOST") match {

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

        (dataSettingsBuilder, Some(adminSettingsBuilder), None)

      case None =>
        val dataSettingsBuilder = BigtableDataSettings.newBuilder()
        val dataSettingsBuilderWithProfileId =
          maybeAppProfileId
            .map(profileId => dataSettingsBuilder.setAppProfileId(profileId))
            .getOrElse(dataSettingsBuilder)
        if (enableUploadClients) {
          val adminSettingsBuilder = BigtableTableAdminSettings.newBuilder()
          val bigQueryClient = BigQueryOptions.getDefaultInstance.getService
          (dataSettingsBuilderWithProfileId, Some(adminSettingsBuilder), Some(bigQueryClient))
        } else {
          (dataSettingsBuilderWithProfileId, None, None)
        }
    }

    // override the bulk read batch settings
    setBigTableBulkReadRowsSettings(dataSettingsBuilder)

    // override thread pools
    setClientThreadPools(dataSettingsBuilder, maybeAdminSettingsBuilder)

    val dataSettings = dataSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
    val dataClient = BigtableDataClient.create(dataSettings)

    val maybeAdminClient = maybeAdminSettingsBuilder.map { adminSettingsBuilder =>
      val adminSettings = adminSettingsBuilder.setProjectId(projectId).setInstanceId(instanceId).build()
      BigtableTableAdminClient.create(adminSettings)
    }

    new BigTableKVStoreImpl(dataClient, maybeAdminClient, maybeBQClient)
  }

  // BigTable's bulk read rows by default will batch calls and wait for a delay before sending them. This is not
  // ideal from a latency perspective, so we set the batching settings to be 1 element and no delay.
  private def setBigTableBulkReadRowsSettings(dataSettingsBuilderWithProfileId: BigtableDataSettings.Builder): Unit = {
    // Get the bulkReadRowsSettings builder
    val bulkReadRowsSettingsBuilder = dataSettingsBuilderWithProfileId
      .stubSettings()
      .bulkReadRowsSettings()

    // Update the batching settings directly on the builder
    bulkReadRowsSettingsBuilder
      .setBatchingSettings(
        bulkReadRowsSettingsBuilder.getBatchingSettings.toBuilder
          .setElementCountThreshold(1)
          .setDelayThresholdDuration(null)
          .build()
      )
  }

  // BigTable's client creates a thread pool with a size of cores * 4. This ends up being a lot larger than we'd like
  // so we scale these down and we also use the same in both clients
  private def setClientThreadPools(
      dataSettingsBuilderWithProfileId: BigtableDataSettings.Builder,
      maybeAdminSettingsBuilder: Option[BigtableTableAdminSettings.Builder]
  ): Unit = {
    dataSettingsBuilderWithProfileId.stubSettings().setBackgroundExecutorProvider(executorProvider)
    maybeAdminSettingsBuilder.foreach(adminSettingsBuilder =>
      adminSettingsBuilder.stubSettings().setBackgroundExecutorProvider(executorProvider))
  }

  // TODO: Load from user jar.
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  //TODO - Implement this
  override def logResponse(resp: LoggableResponse): Unit = {}
}

object GcpApiImpl {
  // Create a thread factory so that we can name the threads for easier debugging
  val threadFactory: ThreadFactory = new ThreadFactory {
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setName(s"chronon-bt-gax-${counter.incrementAndGet()}")
      t
    }
  }

  // create one of these as BT creates very large threadpools (cores * 4) and does them once per admin and data client
  lazy val executorProvider: InstantiatingExecutorProvider = InstantiatingExecutorProvider
    .newBuilder()
    .setExecutorThreadCount(Runtime.getRuntime.availableProcessors())
    .setThreadFactory(threadFactory)
    .build()
}
