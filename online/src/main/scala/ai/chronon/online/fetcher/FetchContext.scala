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

  def getOrCreateExecutionContext: ExecutionContext = {
    Option(executionContextOverride).getOrElse(FlexibleExecutionContext.buildExecutionContext)
  }

  def isTilingEnabled: Boolean = {
    Option(flagStore)
      .map(_.isSet(FlagStoreConstants.TILING_ENABLED, Map.empty[String, String].toJava))
      .exists(_.asInstanceOf[Boolean])
  }
}
