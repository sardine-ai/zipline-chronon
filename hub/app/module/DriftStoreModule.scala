package module

import ai.chronon.online.KVStore
import ai.chronon.online.stats.DriftStore
import com.google.inject.AbstractModule

import javax.inject.Inject
import javax.inject.Provider

class DriftStoreModule extends AbstractModule {

  override def configure(): Unit = {
    // TODO swap to concrete api impl in a follow up
    bind(classOf[DriftStore]).toProvider(classOf[DriftStoreProvider]).asEagerSingleton()
  }
}

class DriftStoreProvider @Inject() (kvStore: KVStore) extends Provider[DriftStore] {
  override def get(): DriftStore = {
    new DriftStore(kvStore)
  }
}
