package ai.chronon.orchestration.service.handlers

import ai.chronon.api.ScalaJavaConversions.{JListOps, ListOps, MapOps}
import ai.chronon.orchestration.persistence.{Conf, ConfDao}
import ai.chronon.orchestration.{DiffRequest, DiffResponse, UploadRequest, UploadResponse}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class UploadHandler(confDao: ConfDao) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Initialize the database tables when the handler is created
  logger.info("Initializing ConfDao tables...")
  Await.result(confDao.createConfTableIfNotExists(), 10.seconds)
  logger.info("ConfDao tables initialized successfully")

  def getDiff(req: DiffRequest): DiffResponse = {
    logger.info(s"Getting diff for ${req.namesToHashes}")

    // Ensure table exists before querying
    val existingConfs = Await.result(confDao.getConfs(), 10.seconds)
    logger.info(s"Found existing $existingConfs")

    // For every conf in the request, check if there is a matching existing conf with the same hash
    // Filter down to only those confs that don't have a match
    val missingConfs = req.namesToHashes.toScala.toMap.filterNot { case (_, hash) =>
      existingConfs.exists(_.confHash == hash)
    }
    val dr = new DiffResponse()
      .setDiff(missingConfs.keys.toList.toJava)
    dr
  }

  def upload(req: UploadRequest): UploadResponse = {
    logger.info(s"Uploading with request: ${req}")

    try {
      val daoConfs = req.diffConfs.toScala.map { conf =>
        Conf(
          conf.getContents,
          conf.getName,
          conf.getHash
        )
      }

      Await.result(confDao.insertConfs(daoConfs.toSeq), 10.seconds)

      new UploadResponse().setMessage("Upload completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Error uploading confs: ${e.getMessage}", e)
        new UploadResponse().setMessage(s"Upload failed: ${e.getMessage}")
    }
  }

}
