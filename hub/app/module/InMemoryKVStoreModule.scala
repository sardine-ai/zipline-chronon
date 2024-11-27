package module

import ai.chronon.api.Constants
import ai.chronon.online.KVStore
import ai.chronon.spark.utils.InMemoryKvStore
import com.google.inject.AbstractModule

// Module that creates and injects an in-memory kv store implementation to allow for quick docker testing
class InMemoryKVStoreModule extends AbstractModule {

  override def configure(): Unit = {
    val inMemoryKVStore = InMemoryKvStore.build("hub", () => null)
    // create relevant datasets in kv store
    inMemoryKVStore.create(Constants.MetadataDataset)
    inMemoryKVStore.create(Constants.TiledSummaryDataset)
    bind(classOf[KVStore]).toInstance(inMemoryKVStore)
  }
}
