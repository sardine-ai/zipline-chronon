package ai.chronon.orchestration.service.handlers

import ai.chronon.api.ScalaJavaConversions.{JListOps, ListOps, MapOps}
import ai.chronon.orchestration.persistence.{Conf, ConfRepoDao}
import ai.chronon.orchestration.{DiffRequest, DiffResponse, UploadRequest, UploadResponse}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class UploadHandler(confRepoDao: ConfRepoDao) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getDiff(req: DiffRequest): DiffResponse = {
    logger.info(s"Getting diff for ${req.namesToHashes}")

    val existingConfs = Await.result(confRepoDao.getConfs(), 10.seconds)

    // For every conf in the request, check if there is a matching existing conf with the same hash
    // Filter down to only those confs that don't have a match
    val missingConfs = req.namesToHashes.toScala.toMap.filterNot { case (_, hash) =>
      existingConfs.exists(_.confHash == hash)
    }
    val dr = new DiffResponse()
      .setDiff(missingConfs.keys.toList.toJava)
    dr
  }

  def upload(req: UploadRequest) = {
    logger.info(s"Uploading ${req.diffConfs.size()} confs")

    val daoConfs = req.diffConfs.toScala.map { conf =>
      Conf(
        conf.getContents, // Todo: how to stringify this?
        conf.getName,
        conf.getHash
      )
    }

    Await.result(confRepoDao.insertConfs(daoConfs.toSeq), 10.seconds)

    new UploadResponse().setMessage("Upload completed successfully")
  }

}
