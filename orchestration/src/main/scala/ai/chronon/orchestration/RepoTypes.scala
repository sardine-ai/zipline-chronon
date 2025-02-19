package ai.chronon.orchestration

import ai.chronon.orchestration.utils.StringExtensions.StringOps

import scala.collection.Seq

/** Types relevant to the orchestration layer.
  * It is very easy to get raw strings mixed up in the indexing logic.
  * So we guard them using case classes. For that reason we have a lot of case classes here.
  */
object RepoTypes {

  /** name of the node
    * example: group_bys.<team>.<file>.<var>, joins.<team>.<file>.<var>, staging_queries.<team>.<file>.<var>
    * and also table.<namespace>.<name> - adding a dummy node for the table makes the code easier to write
    */
  case class Name(name: String)

  case class Branch(name: String)

  object Branch {
    val main: Branch = Branch("main")
  }

  /** Take the file content string and hashes it
    * Whenever this changes the cli will upload the file into the index.
    */
  case class FileHash(hash: String)

  /** Local hash represents the computation defined in the file.
    * In chronon api, anything field other than metadata is
    * considered to impact the computation & consequently the output.
    */
  case class LocalHash(hash: String)

  /** Global hash represents the computation defined in the file and all its dependencies.
    * We recursively scan all the parents of the node to compute the global hash.
    *
    * `global_hash(node) = hash(node.local_hash + node.parents.map(global_hash))`
    */
  case class GlobalHash(hash: String)

  /** Local data represents relevant information for lineage tracking
    * that can be computed by parsing a file in *isolation*.
    */
  case class LocalData(name: Name, fileHash: FileHash, localHash: LocalHash, inputs: Seq[Name], outputs: Seq[Name])

  object LocalData {

    def forArtifact(name: Name, parents: Seq[Name]): LocalData = {

      val nameHash = name.name.md5

      LocalData(
        name,
        FileHash(nameHash),
        LocalHash(nameHash),
        inputs = parents,
        outputs = Seq.empty
      )
    }
  }

  /** Node content represents the actual data that is stored in the index.
    * It is a combination of local data and the actual data that is stored in the index.
    */
  case class Version(name: String)

  /** Content of the compiled file
    * Currently TSimpleJsonProtocol serialized StagingQuery, Join, GroupBy thrift objects.
    * Python compile.py will serialize user's python code into these objects and
    * the [[RepoParser]] will pick them up and sync into [[RepoIndex]].
    */
  case class FileContent(content: String) {
    def hash: FileHash = FileHash(content.md5)
  }

  case class NodeContent[T](localData: LocalData, conf: T)

  case class Table(name: String)

  /** To make the code testable, we parameterize the Config with `T`
    * You can see how this is used in [[RepoIndexSpec]]
    */
  trait ConfProcessor[T] {
    def nodeContents(t: T): Seq[NodeContent[T]]
    def parse(name: String, fileContent: FileContent): Seq[T]
  }
}
