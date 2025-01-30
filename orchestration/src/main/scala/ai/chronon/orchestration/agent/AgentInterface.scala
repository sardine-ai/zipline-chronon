package ai.chronon.orchestration.agent

import ai.chronon.api.JobInfo
import ai.chronon.api.JobListGetRequest
import ai.chronon.api.KvWriteJob
import ai.chronon.api.PartitionListingJob
import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api.YarnJob
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions

case class AgentConfig(
    jobInfoTtl: Long
)

// TODO
class AgentRunner(customerId: String) {

  def init(): Unit = {
    // wake up and inventory running jobs
    val existing = listExisting()
    new JobListGetRequest().setExistingJobsIds(existing.toJava)

    // fetch new jobs from the queue

  }

  def fetchJobs(): Unit = {
    // fetch new jobs from the queue
  }

  def listExisting(): List[String] = {
    // list all jobs that have been running for more than ttl
//    yarnRunner.listJobsIds()
    ??? // TODO
  }

}

// TODO
trait YarnRunner {

  case class YarnJobInstance()

  def init(): Unit

  def setJobInfoTtl(ttlSeconds: Long): Unit

  def runYarnJob(jobId: String, job: YarnJob): YarnJobInstance

  def getJobInfo(jobId: String): JobInfo

  def listJobsIds(): List[String]

}

trait KVWriter {

  case class KVWriteResponse()

  def write(jobId: String, kvWriteJob: KvWriteJob): KVWriteResponse

}

trait PartitionLister {

  case class PartitionListingResponse()

  def init(): Unit

  def read(jobId: String, partitionListingJob: PartitionListingJob): PartitionListingResponse

}

class ControllerClient(customerId: String) {
  private val vertx: Vertx = Vertx.vertx

  private val agentHost = System.getenv("HOSTNAME")

  // TODO figure out auth & tokens & encryption
  private val clientOptions = new WebClientOptions()
    .setUserAgent(s"data-plane/$customerId/$agentHost")
    .setKeepAlive(true)
    .setMaxPoolSize(100)
    .setConnectTimeout(5000) // 5 seconds
    .setSsl(true)

  WebClient.create(vertx, clientOptions)

}
