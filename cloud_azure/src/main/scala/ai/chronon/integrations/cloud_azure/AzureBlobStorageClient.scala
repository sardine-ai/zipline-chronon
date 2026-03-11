package ai.chronon.integrations.cloud_azure

import ai.chronon.spark.submission.StorageClient
import com.azure.storage.blob.BlobServiceClient

import java.io.ByteArrayInputStream
import java.net.URI

class AzureBlobStorageClient(blobServiceClient: BlobServiceClient) extends StorageClient {

  override def downloadObjectToMemory(objectPath: String): Array[Byte] = {
    val (container, blobName) = parsePath(objectPath)
    val blobClient = blobServiceClient.getBlobContainerClient(container).getBlobClient(blobName)
    val outputStream = new java.io.ByteArrayOutputStream()
    blobClient.downloadStream(outputStream)
    outputStream.toByteArray
  }

  override def listFiles(bucketPath: String): Iterator[String] = {
    import scala.jdk.CollectionConverters._
    val (container, prefix) = parsePath(bucketPath)
    blobServiceClient
      .getBlobContainerClient(container)
      .listBlobsByHierarchy(prefix)
      .iterator()
      .asScala
      .map(_.getName)
  }

  override def fileExists(objectPath: String): Boolean = {
    val (container, blobName) = parsePath(objectPath)
    blobServiceClient.getBlobContainerClient(container).getBlobClient(blobName).exists()
  }

  override def upload(objectPath: String, content: Array[Byte]): Unit = {
    val (container, blobName) = parsePath(objectPath)
    val blobClient = blobServiceClient.getBlobContainerClient(container).getBlobClient(blobName)
    blobClient.upload(new ByteArrayInputStream(content), content.length, true)
  }

  private[cloud_azure] def parsePath(path: String): (String, String) = {
    val uri = URI.create(path)
    val container = uri.getUserInfo
    val blobPath = uri.getPath.stripPrefix("/")
    require(container != null && container.nonEmpty,
            s"Expected abfss://container@account.dfs.core.windows.net/path, got: $path")
    require(blobPath.nonEmpty, s"Expected abfss://container@account.dfs.core.windows.net/path, got: $path")
    (container, blobPath)
  }
}
