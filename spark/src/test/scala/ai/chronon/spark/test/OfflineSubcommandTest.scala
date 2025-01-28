/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import ai.chronon.spark.Driver.OfflineSubcommand
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.rogach.scallop.ScallopConf
import org.scalatest.flatspec.AnyFlatSpec
import org.yaml.snakeyaml.Yaml

import scala.io.Source

import collection.JavaConverters._

class OfflineSubcommandTest extends AnyFlatSpec {

  class TestArgs(args: Array[String]) extends ScallopConf(args) with OfflineSubcommand {
    verify()

    override def subcommandName: String = "test"

    override def buildSparkSession(): SparkSession = super.buildSparkSession()

    override def isLocal: Boolean = true
  }

  it should "basic is parsed correctly" in {
    val confPath = "joins/team/example_join.v1"
    val args = new TestArgs(Seq("--conf-path", confPath).toArray)
    assertEquals(confPath, args.confPath())
    assertTrue(args.localTableMapping.isEmpty)
  }

  it should "local table mapping is parsed correctly" in {
    val confPath = "joins/team/example_join.v1"
    val endData = "2023-03-03"
    val argList = Seq("--local-table-mapping", "a=b", "c=d", "--conf-path", confPath, "--end-date", endData)
    val args = new TestArgs(argList.toArray)
    assertTrue(args.localTableMapping.nonEmpty)
    assertEquals("b", args.localTableMapping("a"))
    assertEquals("d", args.localTableMapping("c"))
    assertEquals(confPath, args.confPath())
    assertEquals(endData, args.endDate())
  }

  it should "additional confs parsed correctly" in {
    implicit val formats: Formats = DefaultFormats

    val url = getClass.getClassLoader.getResource("test-driver-additional-confs.yaml")

    val args = new TestArgs(Seq("--conf-path", "does_not_exist", "--additional-conf-path", url.toURI.getPath).toArray)
    val sparkSession = args.buildSparkSession()
    val yamlLoader = new Yaml()

    val confs = Option(getClass.getClassLoader
      .getResourceAsStream("test-driver-additional-confs.yaml"))
      .map(Source.fromInputStream)
      .map((is) =>
        try { is.mkString }
        finally { is.close })
      .map(yamlLoader.load(_).asInstanceOf[java.util.Map[String, Any]])
      .map((jMap) => Extraction.decompose(jMap.asScala.toMap))
      .map((jVal) => render(jVal))
      .map(compact)
      .map(parse(_).extract[Map[String, String]])
      .getOrElse(throw new IllegalArgumentException("Yaml conf not found or invalid yaml"))

    val confKey = "test.yaml.key"
    assertEquals(confs.get(confKey), sparkSession.conf.getOption(confKey))
    assertEquals(Some("test_yaml_key"), sparkSession.conf.getOption(confKey))
    assertTrue(sparkSession.conf.getOption("nonexistent_key").isEmpty)
  }
}
