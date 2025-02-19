package ai.chronon.orchestration

import ai.chronon.orchestration.RepoIndex._
import ai.chronon.orchestration.RepoTypes._
import ai.chronon.orchestration.utils.CollectionExtensions.IteratorExtensions
import ai.chronon.orchestration.utils.SequenceMap
import ai.chronon.orchestration.utils.StringExtensions.StringOps
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable
import scala.collection.Seq

/** Indexer to store and assign versions to nodes in the repo based on their lineage.
  * This also manages versions and contents across multiple branches.
  *
  * Careful consideration has been given to avoid O(N*N) operations in this class.
  *
  * We use a generic conf type T - make it easy to test complex cases.
  *
  * We expect the cli tool to
  *   first, call [[diff]] to see which files are not already present in the index.
  *   second, call [[addFiles]] to add updated/new nodes to the index along with their branch name.
  *           you can also dry-run the [[addFiles]] method to see potential versions that will be assigned.
  *   upon closing of a branch, call [[pruneBranch]] to remove un-used the contents of the branch from the index.
  *
  *   merging into master - is simply calling [[addNodes]] with the new contents of master post merge.
  *
  * NOTE: This class is not thread safe. Only one thread should access this index at a time.
  */
class RepoIndex[T >: Null](proc: ConfProcessor[T]) extends Logging {

  /** We don't duplicate contents of files across branches and commits in the index
    * We just store contents by fileHash and branch to fileHash mapping separately.
    */
  private val branchToFileHash: TriMap[Branch, Name, FileHash] = mutable.Map.empty
  private val fileHashToContent: TriMap[Name, FileHash, NodeContent[T]] = mutable.Map.empty

  /** Versions are globally unique, we compute global hashes for each node based on lineage
    * and then assign a version to it. We store these versions per branch, node in the [[branchVersionIndex]]
    */
  private val branchVersionIndex: TriMap[Branch, Name, Version] = mutable.Map.empty

  /** We use a [[SequenceMap]] to then take this global hash and assign a version to it
    * version entries in the sequencer are never deleted. So that,
    * upon reverting changes we can refer to older version instead
    * of bumping it unnecessarily. This will avoid re-computation of assets.
    */
  private val versionSequencer: SequenceMap[Name, GlobalHash] = new SequenceMap[Name, GlobalHash]

  /** @param fileHashes map of all the file names to their hashes in the branch.
    * @param newNodes nodes that aren't already present in the index. De-duped across branches.
    * @param branch branch on which the user is making the changes.
    * @param dryRun when true shows potential versions that will be assigned to nodes without modifying the index.
    * @return
    *      - a list of version updates that will be applied to the index.
    */
  def addNodes(fileHashes: mutable.Map[Name, FileHash],
               newNodes: Seq[T],
               branch: Branch,
               dryRun: Boolean = true): Seq[VersionUpdate] = {

    val newContents = buildContentMap(proc, newNodes, fileHashes)
    val enrichedFileHashes = newContents.map { case (name, content) =>
      name -> content.localData.fileHash
    } ++ fileHashes

    /** we turn local hash into global hash by combining it with parent hashes recursively
      * global_hash(node) = hash(node.local_hash + node.parents.map(global_hash))
      *
      * we use memoization to avoid recomputing global hashes via the [[globalHashes]] map
      */
    def computeGlobalHash(name: Name, globalHashes: mutable.Map[Name, GlobalHash]): GlobalHash = {

      if (globalHashes.contains(name)) return globalHashes(name)

      val fileHash = enrichedFileHashes.get(name) match {
        case Some(hash) => hash

        // this could be an artifact related to unchanged files on the branch
        // so we reach out to content index
        // artifacts are just names with no content - so there should be just one entry
        case None =>
          val hashToContent = fileHashToContent(name)

          require(hashToContent.size == 1, s"Expected 1 entry for artifact $name, found ${hashToContent.size}")
          require(hashToContent.head._2.localData.fileHash.hash == name.name.md5,
                  s"Expected artifact $name to have no inputs")

          hashToContent.head._1
      }

      val content = if (newContents.contains(name)) {
        newContents(name)
      } else {
        fileHashToContent(name)(fileHash)
      }

      val localHash = content.localData.localHash
      val parents = content.localData.inputs

      // recursively compute parent hashes
      val parentHashes = parents
        .map { parent =>
          val parentHash = computeGlobalHash(parent, globalHashes).hash
          s"${parent.name}:$parentHash"
        }
        .mkString(",")

      // combine parent hashcode with local hash
      val codeString = s"node=${name.name}:${localHash.hash}|parents=$parentHashes"

      logger.info(s"Global Hash elements: $codeString")

      val globalHash = GlobalHash(codeString.md5)

      globalHashes.update(name, globalHash)
      globalHash
    }

    val globalHashes = mutable.Map.empty[Name, GlobalHash]
    // this line fills global hashes
    fileHashes.foreach { case (name, _) => computeGlobalHash(name, globalHashes) }

    val existingVersions = branchVersionIndex.getOrElse(branch, mutable.Map.empty)
    val mainVersions = branchVersionIndex.getOrElse(Branch.main, mutable.Map.empty)

    if (!dryRun) {

      logger.info("Not a dry run! Inserting new nodes into the index into branch: " + branch.name)

      newContents.foreach { case (name, content) =>
        update(fileHashToContent, name, content.localData.fileHash, content)
      }

      val newVersions = globalHashes.map { case (name, globalHash) =>
        val versionIndex = versionSequencer.insert(name, globalHash)
        val version = Version("v" + versionIndex.toString)
        name -> version
      }

      branchToFileHash.update(branch, enrichedFileHashes)
      branchVersionIndex.update(branch, newVersions)

      VersionUpdate.join(newVersions, existingVersions, mainVersions)

    } else {

      // dry run - don't insert into any members of the index
      val newVersions = mutable.Map.empty[Name, Version]
      globalHashes.foreach { case (name, globalHash) =>
        val versionIndex = versionSequencer.potentialIndex(name, globalHash)
        newVersions.update(name, Version("v" + versionIndex.toString))
      }

      VersionUpdate.join(newVersions, existingVersions, mainVersions)
    }
  }

  def addFiles(fileHashes: mutable.Map[Name, FileHash], updatedFiles: Map[String, String], branch: Branch): Unit = {

    val nodes: Seq[T] = updatedFiles.iterator
      .flatMap { case (name, content) =>
        proc.parse(name, FileContent(content))
      }
      .distinct
      .toSeq

    addNodes(fileHashes, nodes, branch)
  }

  /** returns the names of the files whose contents haven't been found in the index across any of the versions
    */
  def diff(incomingFileHashes: mutable.Map[Name, FileHash]): Seq[Name] = {

    incomingFileHashes
      .filter { case (name, incomingHash) =>
        val fileHashMap = fileHashToContent.get(name)

        lazy val nameAbsentInIndex = fileHashMap.isEmpty
        lazy val fileHashAbsentForName = !fileHashMap.get.contains(incomingHash)

        nameAbsentInIndex || fileHashAbsentForName

      }
      .keys
      .toSeq
  }

  /** Removes contents and index entries related to a branch
    */
  def pruneBranch(branch: Branch): Unit = {

    branchToFileHash.remove(branch)
    branchVersionIndex.remove(branch)

    pruneContents()

  }

  /** Removes contents that are not referred to via [[FileHash]] in any branch
    */
  private def pruneContents(): Unit = {

    // collect unique hashes per name from every branch
    val validHashes: mutable.Map[Name, mutable.HashSet[FileHash]] = innerKeyToValueSet(branchToFileHash)

    fileHashToContent.retain { case (name, fileHashMap) =>
      fileHashMap.retain { case (fileHash, _) =>
        validHashes.get(name) match {
          case None         => false // no branch has this name
          case Some(hashes) => hashes.contains(fileHash) // this branch has this fileHash
        }

      }

      fileHashMap.nonEmpty
    }
  }
}

object RepoIndex {

  private type TriMap[K1, K2, V] = mutable.Map[K1, mutable.Map[K2, V]]

  private def update[K1, K2, V](map: TriMap[K1, K2, V], k1: K1, k2: K2, v: V): Unit =
    map.getOrElseUpdate(k1, mutable.Map.empty).update(k2, v)

  private def innerKeyToValueSet[K1, K2, V](map: TriMap[K1, K2, V]): mutable.Map[K2, mutable.HashSet[V]] = {
    val result = mutable.Map.empty[K2, mutable.HashSet[V]]
    map.values.foreach { innerMap =>
      innerMap.foreach { case (k2, v) =>
        result.getOrElseUpdate(k2, mutable.HashSet.empty).add(v)
      }
    }
    result
  }

  /** Takes data from repo parser and builds a local index for the repo parser
    * We treat inputs and outputs that are not present in FileHashes as artifacts
    * For these artifacts we create additional entries in the result
    */
  def buildContentMap[T >: Null](proc: ConfProcessor[T],
                                 nodes: Seq[T],
                                 fileHashes: mutable.Map[Name, FileHash]): mutable.Map[Name, NodeContent[T]] = {

    val contentMap = mutable.Map.empty[Name, NodeContent[T]]

    // first pass - update non-artifact contents
    for (
      node <- nodes;
      nodeContent <- proc.nodeContents(node)
    ) {

      val name = nodeContent.localData.name
      contentMap.update(name, nodeContent)

      def updateContents(artifactName: Name, isOutput: Boolean): Unit = {

        // artifacts are not present in file hashes
        if (fileHashes.contains(artifactName)) return

        val existingParents = if (contentMap.contains(artifactName)) {
          contentMap(artifactName).localData.inputs
        } else {
          Seq.empty
        }

        val newParents = if (isOutput) Seq(name) else Seq.empty

        val parents = (existingParents ++ newParents).distinct

        val artifactData = LocalData.forArtifact(artifactName, parents)
        val artifactContent = NodeContent[T](artifactData, null)

        contentMap.update(artifactName, artifactContent)

      }

      nodeContent.localData.outputs.foreach { output => updateContents(output, isOutput = true) }
      nodeContent.localData.inputs.foreach { input => updateContents(input, isOutput = false) }

    }

    contentMap
  }

}
