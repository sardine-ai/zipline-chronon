package ai.chronon.agent

import ai.chronon.api.{Job, JobStatusType}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/** KVStore implementation that uses separate datasets for active vs completed jobs
  * to reduce range scan costs and improve performance when retrieving active jobs.
  */
class KVJobStore(kvStore: KVStore) extends JobStore {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // TODO: Move these to config
  private val activeJobsDataset = "AGENT_ACTIVE_JOBS"
  private val completedJobsDataset = "AGENT_COMPLETED_JOBS"
  private val timeoutMs = 10000

  // Ensure the datasets exist
  kvStore.create(activeJobsDataset)
  kvStore.create(completedJobsDataset)

  implicit val ec: ExecutionContext = kvStore.executionContext

  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  private def serializeJob(job: Job): Array[Byte] = {
    try {
      objectMapper.writeValueAsBytes(job)
    } catch {
      case e: Exception =>
        logger.error("Error serializing job", e)
        throw e
    }
  }

  private def deserializeJob(bytes: Array[Byte]): Try[Job] = {
    Try {
      objectMapper.readValue(bytes, classOf[Job])
    }
  }

  private def isJobActive(job: Job): Boolean = {
    val status = job.getJobInfo.getCurrentStatus
    // Jobs are active if they are pending, running, or unknown
    // They're considered completed if they succeeded, failed, or were cancelled
    status == JobStatusType.PENDING ||
    status == JobStatusType.RUNNING ||
    status == JobStatusType.UNKNOWN
  }

  override def storeJob(jobId: String, job: Job): Unit = {
    try {
      val jobBytes = serializeJob(job)
      // Store in active or completed dataset based on status
      val dataset = if (isJobActive(job)) activeJobsDataset else completedJobsDataset
      val putRequest = PutRequest(jobId.getBytes(StandardCharsets.UTF_8), jobBytes, dataset)

      val result = Await.result(kvStore.put(putRequest), timeoutMs.millis)
      if (!result) {
        logger.error(s"Failed to store job $jobId")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error storing job $jobId", e)
        throw e
    }
  }

  override def getJob(jobId: String): Option[Job] = {
    // First try to find the job in the active jobs dataset
    getJobFromDataset(jobId, activeJobsDataset).orElse {
      // If not found, try the completed jobs dataset
      getJobFromDataset(jobId, completedJobsDataset)
    }
  }

  private def getJobFromDataset(jobId: String, dataset: String): Option[Job] = {
    try {
      val getRequest = GetRequest(jobId.getBytes(StandardCharsets.UTF_8), dataset)
      val responseFuture = kvStore.get(getRequest)
      val response = Await.result(responseFuture, timeoutMs.millis)

      response.values match {
        case Success(values) if values.nonEmpty =>
          val latestValue = values.maxBy(_.millis)
          deserializeJob(latestValue.bytes) match {
            case Success(job) => Some(job)
            case Failure(e) =>
              logger.error(s"Failed to deserialize job $jobId from $dataset", e)
              None
          }
        case Failure(e) =>
          logger.error(s"Error retrieving job $jobId from $dataset", e)
          None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error getting job $jobId from $dataset", e)
        None
    }
  }

  override def getAllActiveJobs: List[Job] = {
    try {
      val listRequest = KVStore.ListRequest(activeJobsDataset, Map.empty)
      val listResponseFuture = kvStore.list(listRequest)
      val listResponse = Await.result(listResponseFuture, timeoutMs.millis)

      listResponse.values match {
        case Success(values) =>
          values.flatMap { listValue =>
            deserializeJob(listValue.valueBytes) match {
              case Success(job) => Some(job)
              case Failure(e) =>
                logger.error(s"Failed to deserialize job from list", e)
                None
            }
          }.toList
        case Failure(e) =>
          logger.error("Failed to list active jobs", e)
          List.empty
      }
    } catch {
      case e: Exception =>
        logger.error("Error listing active jobs", e)
        List.empty
    }
  }

  /** Uses tombstoning to mark records as deleted since KVStore lacks a direct delete operation.
    * This prevents stale job entries from persisting when jobs move between datasets.
    */
  private def deleteJobFromDataset(jobId: String, dataset: String): Unit = {
    try {
      // Create an empty value (this is effectively a "tombstone" marking the entry as deleted)
      val emptyValueRequest = PutRequest(
        jobId.getBytes(StandardCharsets.UTF_8),
        Array.emptyByteArray,
        dataset,
        Some(System.currentTimeMillis())
      )

      val result = Await.result(kvStore.put(emptyValueRequest), timeoutMs.millis)
      if (!result) {
        logger.error(s"Failed to delete job $jobId from $dataset")
      } else {
        logger.info(s"Successfully deleted job $jobId from $dataset")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error deleting job $jobId from $dataset", e)
    }
  }

  override def updateJobStatus(jobId: String, status: JobStatusType): Unit = {
    try {
      getJob(jobId) match {
        case Some(job) =>
          val oldStatus = job.getJobInfo.getCurrentStatus
          val wasActive = isJobActive(job)

          // Update the status
          job.getJobInfo.setCurrentStatus(status)
          val isNowActive = isJobActive(job)

          // Check if we need to move between active and completed tables
          if (wasActive != isNowActive) {
            logger.info(s"Job $jobId moved from status $oldStatus to $status, moving between datasets")

            // Delete from the old dataset by writing a tombstone
            val oldDataset = if (wasActive) activeJobsDataset else completedJobsDataset
            deleteJobFromDataset(jobId, oldDataset)

            // Store in the new dataset (will use the correct dataset based on status)
            storeJob(jobId, job)
          } else {
            // Just update the job in the same dataset
            storeJob(jobId, job)
          }
        case None =>
          logger.warn(s"Cannot update status for job $jobId: job not found")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error updating status for job $jobId", e)
    }
  }
}
