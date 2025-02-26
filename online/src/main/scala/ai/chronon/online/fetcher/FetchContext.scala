package ai.chronon.online.fetcher
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.ScalaJavaConversions.JMapOps
import ai.chronon.online.{FlagStore, FlagStoreConstants, FlexibleExecutionContext, KVStore}

import scala.concurrent.ExecutionContext

case class FetchContext(kvStore: KVStore,
                        metadataDataset: String = MetadataDataset,
                        timeoutMillis: Long = 10000,
                        debug: Boolean = false,
                        flagStore: FlagStore = null,
                        disableErrorThrows: Boolean = false,
                        executionContextOverride: ExecutionContext = null) {

  def isTilingEnabled: Boolean = {
    Option(flagStore)
      .map(_.isSet(FlagStoreConstants.TILING_ENABLED, Map.empty[String, String].toJava))
      .exists(_.asInstanceOf[Boolean])
  }

  def shouldStreamingDecodeThrow(groupByName: String): Boolean = {
    Option(flagStore)
      .exists(
        _.isSet("disable_streaming_decoding_error_throws", Map("group_by_streaming_dataset" -> groupByName).toJava))
  }

  def getOrCreateExecutionContext: ExecutionContext = {
    Option(executionContextOverride).getOrElse(FlexibleExecutionContext.buildExecutionContext)
  }
}
