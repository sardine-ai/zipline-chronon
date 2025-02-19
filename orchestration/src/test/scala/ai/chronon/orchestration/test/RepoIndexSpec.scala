package ai.chronon.orchestration.test

import ai.chronon.orchestration.RepoIndex
import ai.chronon.orchestration.RepoTypes._
import ai.chronon.orchestration.VersionUpdate
import ai.chronon.orchestration.utils.StringExtensions.StringOps
import org.apache.logging.log4j.scala.Logging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.Seq

class RepoIndexSpec extends AnyFlatSpec with Matchers with Logging {

  case class TestConf(name: String,
                      queryVersion: String,
                      params: String,
                      parents: Seq[String],
                      outputs: Seq[String] = Seq.empty)

  class TestConfProcessor extends ConfProcessor[TestConf] {

    override def nodeContents(conf: TestConf): Seq[NodeContent[TestConf]] = {

      val ld = LocalData(
        name = Name(conf.name),
        fileHash = FileHash(conf.toString.md5),
        localHash = LocalHash(conf.queryVersion.md5),
        inputs = conf.parents.map(Name),
        outputs = conf.outputs.map(Name)
      )

      Seq(NodeContent(ld, conf))
    }

    override def parse(name: String, fileContent: FileContent): Seq[TestConf] = ???
  }

  "RepoIndex" should "propagate updates" in {

    val proc = new TestConfProcessor
    val repoIndex = new RepoIndex[TestConf](proc)

    def fileHashes(configs: Seq[TestConf]): mutable.Map[Name, FileHash] = {
      val nameHashPairs = configs
        .flatMap(proc.nodeContents)
        .map(nc => nc.localData.name -> nc.localData.fileHash)
      mutable.Map(nameHashPairs.toSeq: _*)
    }

    def updateIndex(confsWithExpectedVersions: Seq[(TestConf, String)],
                    branch: Branch,
                    commitMessage: String): Seq[VersionUpdate] = {
      logger.info(s"Updating index branch @${branch.name} and commit - $commitMessage")

      val expectedVersions = confsWithExpectedVersions.map(c => c._1.name -> c._2).toMap

      val confs = confsWithExpectedVersions.map(_._1)
      val fileHashMap = fileHashes(confs)
      val diffNodes = repoIndex.diff(fileHashMap).map(_.name).toSet

      logger.info("incoming files:\n      " + fileHashMap.keySet.mkString("\n      "))
      logger.info("diff nodes:\n      " + diffNodes.mkString("\n      "))

      val updates =
        repoIndex.addNodes(fileHashMap, confs.filter(c => diffNodes.contains(c.name)), branch, dryRun = false)

      val actualVersions = VersionUpdate.toMap(updates)

      expectedVersions.foreach { case (name, expectedVersion) =>
        actualVersions.get(name) shouldBe Some(expectedVersion)
      }

      VersionUpdate.print(updates)
      logger.info(s"Finished adding commit: $commitMessage\n\n")
      updates
    }

    val confs = Seq(
      TestConf("sq1", "v1", "4g", Seq.empty, Seq("t1")) -> "v0",
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v0",
      TestConf("gb2", "v1", "4g", Seq("t2")) -> "v0",
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2"), Seq("table_j1")) -> "v0",
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v0"
    )

    val fileHashMap = fileHashes(confs.map(_._1))

    // check artifact nodes are present
    val map = RepoIndex.buildContentMap(proc, confs.map(_._1), fileHashMap)
    map.get(Name("t1")) shouldNot be(None)
    map.get(Name("t2")) shouldNot be(None)

    logger.info(s"fileHashMap: $fileHashMap")

    val fileDiffs = repoIndex.diff(fileHashMap)

    fileDiffs.size shouldBe fileHashMap.size
    fileDiffs.toSet shouldBe fileHashMap.keySet

    updateIndex(confs, Branch.main, "initial commit")

    val testBranch = Branch("test")

    val branchConfs1 = Seq(
      TestConf("sq1", "v2", "4g", Seq.empty, Seq("t1")) -> "v1", // updated
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v1",
      TestConf("gb2", "v1", "4g", Seq("t2")) -> "v0",
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2"), Seq("table_j1")) -> "v1",
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v1"
    )

    updateIndex(branchConfs1, testBranch, "semantically updated sq1")

    val branchConfs2 = Seq(
      TestConf("sq1", "v2", "4g", Seq.empty, Seq("t1")) -> "v1",
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v1",
      TestConf("gb2", "v1", "8g", Seq("t2")) -> "v0", // non-semantic update
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2"), Seq("table_j1")) -> "v1",
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v1"
    )

    updateIndex(branchConfs2, testBranch, "non semantically updated gb2")

    val branchConfs3 = Seq(
      TestConf("sq1", "v1", "4g", Seq.empty, Seq("t1")) -> "v0", // reverted back
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v0",
      TestConf("gb2", "v1", "8g", Seq("t2")) -> "v0",
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2"), Seq("table_j1")) -> "v0",
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v0"
    )
    updateIndex(branchConfs3, testBranch, "reverted back semantic update to sq1")

    val branchConfs4 = Seq(
      TestConf("sq1", "v1", "4g", Seq.empty, Seq("t1")) -> "v0",
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v0",
      // TestConf("gb2", "v1", "8g", Seq("t2")), // deleted
      TestConf("j1", "v1", "4g", Seq("gb1"), Seq("table_j1")) -> "v2", // parent deleted
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v2"
    )

    updateIndex(branchConfs4, testBranch, "deleted gb2 (depends on t2)")

    updateIndex(branchConfs4, Branch.main, "updated main with change in test branch")

    val branchConfs5 = Seq(
      TestConf("sq1", "v1", "4g", Seq.empty, Seq("t1")) -> "v0",
      TestConf("sq3", "v1", "4g", Seq.empty, Seq("t3")) -> "v0", // new
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v0",
      TestConf("gb3", "v1", "4g", Seq("t3")) -> "v0", // new
      TestConf("gb2", "v1", "8g", Seq("t2")) -> "v0", // gb2 added back
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2", "gb3"), Seq("table_j1")) -> "v3", // parent reverted + new
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v3"
    )

    updateIndex(branchConfs5, Branch.main, "new sq3 and gb3, un-deleted gb2")

    val branchConfs6 = Seq(
      TestConf("sq1", "v1", "4g", Seq.empty, Seq("t1")) -> "v0",
      TestConf("sq3", "v1", "4g", Seq.empty, Seq("t3")) -> "v0",
      TestConf("gb1", "v1", "4g", Seq("t1")) -> "v0",
      TestConf("gb3", "v1", "4g", Seq("t3")) -> "v0",
      TestConf("gb2", "v1", "8g", Seq("t2")) -> "v0",
      TestConf("j1", "v1", "4g", Seq("gb1", "gb2", "gb3"), Seq("table_j1")) -> "v3",
      TestConf("m1", "v1", "4g", Seq("j1"), Seq("table_m1")) -> "v3",
      TestConf("m2", "v1", "4g", Seq("j1"), Seq("table_m2")) -> "v0"
    )

    updateIndex(branchConfs6, Branch.main, "m2 is added")
  }

}
