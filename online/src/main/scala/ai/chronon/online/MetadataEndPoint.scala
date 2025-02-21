package ai.chronon.online

import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.GroupBy
import ai.chronon.api.Join
import ai.chronon.api.MetaData
import ai.chronon.api.Model
import ai.chronon.api.StagingQuery
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

case class MetadataEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](
    extractFn: (String, Conf) => (String, String),
    name: String
)
object MetadataEndPoint {
  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val ConfByKeyEndPointName = "CHRONON_METADATA"
  val NameByTeamEndPointName = "CHRONON_ENTITY_BY_TEAM"

  private def getTeamFromMetadata(metaData: MetaData): String = {
    val team = metaData.team
    if (metaData.customJson != null && metaData.customJson.nonEmpty) {
      implicit val formats = DefaultFormats
      val customJson = parse(metaData.customJson)
      val teamFromJson: String = (customJson \ "team_override").extractOpt[String].getOrElse("")
      if (teamFromJson.nonEmpty) teamFromJson else team
    } else team
  }

  private def parseTeam[Conf <: TBase[_, _]: Manifest: ClassTag](conf: Conf): String = {
    conf match {
      case join: Join                 => "joins/" + getTeamFromMetadata(join.metaData)
      case groupBy: GroupBy           => "group_bys/" + getTeamFromMetadata(groupBy.metaData)
      case stagingQuery: StagingQuery => "staging_queries/" + getTeamFromMetadata(stagingQuery.metaData)
      case model: Model               => "models/" + getTeamFromMetadata(model.metaData)
      case _ =>
        logger.error(s"Failed to parse team from $conf")
        throw new Exception(s"Failed to parse team from $conf")
    }
  }

  // key: entity path, e.g. joins/team/team.example_join.v1
  // value: entity config in json format
  private def confByKeyEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (metadataName, conf) => (metadataName, ThriftJsonCodec.toJsonStr(conf)),
      name = ConfByKeyEndPointName
    )

  // key: entity type + team name, e.g. joins/team
  // value: list of entities under the team, e.g. joins/team/team.example_join.v1, joins/team/team.example_join.v2
  private def NameByTeamEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag] =
    new MetadataEndPoint[Conf](
      extractFn = (path, conf) => (parseTeam[Conf](conf), path.confPathToKey),
      name = NameByTeamEndPointName
    )

  def getEndPoint[Conf <: TBase[_, _]: Manifest: ClassTag](endPointName: String): MetadataEndPoint[Conf] = {
    endPointName match {
      case ConfByKeyEndPointName  => confByKeyEndPoint[Conf]
      case NameByTeamEndPointName => NameByTeamEndPoint[Conf]
      case _ =>
        logger.error(s"Failed to find endpoint for $endPointName")
        throw new Exception(s"Failed to find endpoint for $endPointName")
    }
  }
}
