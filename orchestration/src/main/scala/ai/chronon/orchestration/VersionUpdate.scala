package ai.chronon.orchestration

import ai.chronon.orchestration.RepoTypes._
import ai.chronon.orchestration.utils.TablePrinter

import scala.collection.mutable
import scala.collection.Seq

case class VersionUpdate(name: Name, previous: Option[Version], next: Option[Version], main: Option[Version]) {

  private def toRow: Seq[String] =
    Seq(
      name.name,
      next.map(_.name).getOrElse(""),
      previous.map(_.name).getOrElse(""),
      main.map(_.name).getOrElse("")
    )
}

object VersionUpdate {

  def join(next: mutable.Map[Name, Version],
           previous: mutable.Map[Name, Version],
           main: mutable.Map[Name, Version]): Seq[VersionUpdate] = {

    val allNames = previous.keySet ++ next.keySet ++ main.keySet

    allNames
      .map { name =>
        VersionUpdate(name, previous.get(name), next.get(name), main.get(name))
      }
      .toSeq
      .sortBy(_.name.name)
  }

  def toMap(versionUpdates: Seq[VersionUpdate]): Map[String, String] = {

    versionUpdates.map { versionUpdate =>
      versionUpdate.name.name -> versionUpdate.next.map(_.name).getOrElse("")
    }.toMap

  }

  def print(versionUpdates: Seq[VersionUpdate]): Unit = {

    val header = Seq("Name", "Next", "Previous", "Main")
    TablePrinter.printTypedTable(header, versionUpdates)(_.toRow)

  }
}
