package ai.chronon.spark.submission

trait StorageClient {

  def downloadObjectToMemory(objectPath: String): Array[Byte]

  def listFiles(bucketPath: String): Iterator[String]

  def fileExists(objectPath: String): Boolean

  def upload(objectPath: String, content: Array[Byte]): Unit

}
