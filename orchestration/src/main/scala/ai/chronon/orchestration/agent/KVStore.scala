package ai.chronon.orchestration.agent

import ai.chronon.api.{Job, JobStatusType}

/** Trait representing a key-value store service.
  *
  * This service provides abstraction for key-value storage operations,
  * allowing the application to work with different storage implementations.
  *
  * TODO: To use our chronon KVStore, extend if needed and have implementations for both GCP and AWS
  * Just a place holder will change a lot in coming PRs
  */
trait KVStore {

  /** Stores a job with the given ID.
    *
    * @param jobId The unique identifier for the job
    * @param job The job object to store
    */
  def storeJob(jobId: String, job: Job): Unit

  /** Retrieves a job with the given ID.
    *
    * @param jobId The unique identifier for the job
    * @return Option containing the job if found, None otherwise
    */
  def getJob(jobId: String): Option[Job]

  /** Retrieves all jobs stored in the system.
    *
    * @return List of all jobs
    */
  def getAllJobs: List[Job]

  /** Updates the status of a job with the given ID.
    *
    * @param jobId The unique identifier for the job
    * @param status The new status of the job
    */
  def updateJobStatus(jobId: String, status: JobStatusType): Unit
}

/** In-memory implementation of KVStore for testing and development.
  */
class InMemoryKVStore extends KVStore {
  private val store = scala.collection.mutable.Map[String, Job]()

  override def storeJob(jobId: String, job: Job): Unit = {
    store.put(jobId, job)
  }

  override def getJob(jobId: String): Option[Job] = {
    store.get(jobId)
  }

  override def getAllJobs: List[Job] = {
    store.values.toList
  }

  override def updateJobStatus(jobId: String, status: JobStatusType): Unit = {
    store.get(jobId).foreach { job =>
      job.jobInfo.setCurrentStatus(status)
    }
  }
}

object KVStore {

  /** Factory method to create an in-memory KVStore instance.
    * @return An in-memory KVStore implementation
    */
  def createInMemory(): KVStore = new InMemoryKVStore()
}
