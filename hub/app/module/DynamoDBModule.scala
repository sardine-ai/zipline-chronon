package module

import ai.chronon.integrations.aws.AwsApiImpl
import com.google.inject.AbstractModule
import play.api.Configuration
import play.api.Environment
import store.DynamoDBMonitoringStore

class DynamoDBModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  override def configure(): Unit = {
    val awsApiImpl = new AwsApiImpl(Map.empty)
    val dynamoDBMonitoringStore = new DynamoDBMonitoringStore(awsApiImpl)
    bind(classOf[DynamoDBMonitoringStore]).toInstance(dynamoDBMonitoringStore)
  }
}
