package module

import com.google.inject.AbstractModule
import ai.chronon.integrations.aws.AwsApiImpl
import play.api.{Configuration, Environment}
import store.DynamoDBMonitoringStore

class DynamoDBModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  override def configure(): Unit = {
    val awsApiImpl = new AwsApiImpl(Map.empty)
    val dynamoDBMonitoringStore = new DynamoDBMonitoringStore(awsApiImpl)
    bind(classOf[DynamoDBMonitoringStore]).toInstance(dynamoDBMonitoringStore)
  }
}
