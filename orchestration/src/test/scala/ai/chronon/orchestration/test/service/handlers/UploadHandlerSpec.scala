package ai.chronon.orchestration.test.service.handlers

import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.orchestration.persistence.{Conf, ConfDao}
import ai.chronon.orchestration.service.handlers.UploadHandler
import ai.chronon.orchestration.{DiffRequest, UploadRequest}
import ai.chronon.orchestration
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future
import scala.language.postfixOps

/** Unit tests for the UploadHandler class */
class UploadHandlerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "UploadHandler.getDiff" should "return configurations that need to be updated" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up existing configurations in the database
    val existingConfs = Seq(
      Conf("content1", "config1", "hash1"),
      Conf("content2", "config2", "hash2")
    )
    when(mockConfDao.getConfs).thenReturn(Future.successful(existingConfs))

    // Create test request with mixed hashes (some match, some don't)
    val request = new DiffRequest()
    val namesToHashes = Map(
      "config1" -> "hash1", // Match - should not be in diff
      "config2" -> "modifiedhash", // Different hash - should be in diff
      "config3" -> "hash3" // New config - should be in diff
    ).toJava
    request.setNamesToHashes(namesToHashes)

    // Create handler and call getDiff
    val handler = new UploadHandler(mockConfDao)
    val response = handler.getDiff(request)

    // Verify response contains only configs that need updating
    response.getDiff.toScala should contain theSameElementsAs Seq("config2", "config3")

    // Verify dao was called
    verify(mockConfDao).getConfs
  }

  it should "return empty diff when all configurations are up to date" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up existing configurations in the database
    val existingConfs = Seq(
      Conf("content1", "config1", "hash1"),
      Conf("content2", "config2", "hash2")
    )
    when(mockConfDao.getConfs).thenReturn(Future.successful(existingConfs))

    // Create test request with all matching hashes
    val request = new DiffRequest()
    val namesToHashes = Map(
      "config1" -> "hash1",
      "config2" -> "hash2"
    ).toJava
    request.setNamesToHashes(namesToHashes)

    // Create handler and call getDiff
    val handler = new UploadHandler(mockConfDao)
    val response = handler.getDiff(request)

    // Verify response contains empty diff
    response.getDiff.isEmpty shouldBe true

    // Verify dao was called
    verify(mockConfDao).getConfs
  }

  it should "handle empty request" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up existing configurations in the database
    val existingConfs = Seq(
      Conf("content1", "config1", "hash1"),
      Conf("content2", "config2", "hash2")
    )
    when(mockConfDao.getConfs).thenReturn(Future.successful(existingConfs))

    // Create empty request
    val request = new DiffRequest()
    request.setNamesToHashes(Map.empty[String, String].toJava)

    // Create handler and call getDiff
    val handler = new UploadHandler(mockConfDao)
    val response = handler.getDiff(request)

    // Verify response contains empty diff
    response.getDiff.isEmpty shouldBe true

    // Verify dao was called
    verify(mockConfDao).getConfs
  }

  it should "handle database errors" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up dao to throw exception
    when(mockConfDao.getConfs).thenReturn(Future.failed(new RuntimeException("Database error")))

    // Create test request
    val request = new DiffRequest()
    val namesToHashes = Map(
      "config1" -> "hash1",
      "config2" -> "hash2"
    ).toJava
    request.setNamesToHashes(namesToHashes)

    // Create handler and call getDiff - should throw exception
    val handler = new UploadHandler(mockConfDao)

    // Verify the exception is propagated
    val exception = intercept[RuntimeException] {
      handler.getDiff(request)
    }

    // Verify exception message
    exception.getMessage should include("Database error")

    // Verify dao was called
    verify(mockConfDao).getConfs
  }

  "UploadHandler.upload" should "insert configurations successfully" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up successful insertion response
    when(mockConfDao.insertConfs(any[Seq[Conf]])).thenReturn(Future.successful(Some(2)))

    // Create test request with configurations to upload
    val request = new UploadRequest()
    val conf1 = new orchestration.Conf()
      .setContents("content1")
      .setName("config1")
      .setHash("hash1")

    val conf2 = new orchestration.Conf()
      .setContents("content2")
      .setName("config2")
      .setHash("hash2")

    request.setDiffConfs(Seq(conf1, conf2).toJava)

    // Create handler and call upload
    val handler = new UploadHandler(mockConfDao)
    val response = handler.upload(request)

    // Verify response indicates success
    response.getMessage should include("completed successfully")

    // Verify dao was called with correct configurations
    verify(mockConfDao).insertConfs(any[Seq[Conf]])
  }

  it should "handle empty upload request" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up successful insertion response
    when(mockConfDao.insertConfs(any[Seq[Conf]])).thenReturn(Future.successful(Some(0)))

    // Create empty upload request
    val request = new UploadRequest()
    request.setDiffConfs(Seq.empty.toJava)

    // Create handler and call upload
    val handler = new UploadHandler(mockConfDao)
    val response = handler.upload(request)

    // Verify response indicates success
    response.getMessage should include("completed successfully")

    // Verify dao was called with empty configuration list
    verify(mockConfDao).insertConfs(any[Seq[Conf]])
  }

  it should "handle database errors during upload" in {
    // Mock dependencies
    val mockConfDao = mock[ConfDao]

    // Set up dao to throw exception
    when(mockConfDao.insertConfs(any[Seq[Conf]])).thenReturn(Future.failed(new RuntimeException("Insert error")))

    // Create test request
    val request = new UploadRequest()
    val conf = new orchestration.Conf()
      .setContents("content")
      .setName("config")
      .setHash("hash")

    request.setDiffConfs(Seq(conf).toJava)

    // Create handler and call upload - should throw exception
    val handler = new UploadHandler(mockConfDao)

    // Verify the exception is propagated
    val exception = intercept[RuntimeException] {
      handler.upload(request)
    }

    // Verify exception message
    exception.getMessage should include("Insert error")

    // Verify dao was called
    verify(mockConfDao).insertConfs(any[Seq[Conf]])
  }
}
