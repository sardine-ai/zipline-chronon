package module

import ai.chronon.integrations.aws.AwsApiImpl
import com.google.inject.AbstractModule
import play.api.Configuration
import play.api.Environment
import store.MonitoringModelStore

class ModelStoreModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  override def configure(): Unit = {
    val awsApiImpl = new AwsApiImpl(Map.empty)
    val dynamoDBMonitoringStore = new MonitoringModelStore(awsApiImpl)
    bind(classOf[MonitoringModelStore]).toInstance(dynamoDBMonitoringStore)
  }
}
