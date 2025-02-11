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

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import ai.chronon.online.Api
import ai.chronon.online.FetcherMain
import ai.chronon.online.MetadataDirWalker
import ai.chronon.online.MetadataEndPoint
import ai.chronon.online.MetadataStore
import ai.chronon.online.TopicChecker
import ai.chronon.spark.stats.CompareBaseJob
import ai.chronon.spark.stats.CompareJob
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.stats.drift.Summarizer
import ai.chronon.spark.stats.drift.SummaryPacker
import ai.chronon.spark.stats.drift.SummaryUploader
import ai.chronon.spark.streaming.JoinSourceRunner
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.Subcommand
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader

// useful to override spark.sql.extensions args - there is no good way to unset that conf apparently
// so we give it dummy extensions
class DummyExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {}
}

// The mega chronon cli
object Driver {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = true)

  trait SharedSubCommandArgs {
    this: ScallopConf =>
    val isGcp: ScallopOption[Boolean] =
      opt[Boolean](required = false, default = Some(false), descr = "Whether to use GCP")
    val gcpProjectId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP project id")
    val gcpBigtableInstanceId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP BigTable instance id")

    val confType: ScallopOption[String] =
      opt[String](required = false, descr = "Type of the conf to run. ex: join, group-by, etc")
  }

  trait OfflineSubcommand extends SharedSubCommandArgs {
    this: ScallopConf =>
    val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to conf")

    val additionalConfPath: ScallopOption[String] =
      opt[String](required = false, descr = "Path to additional driver job configurations")

    val runFirstHole: ScallopOption[Boolean] =
      opt[Boolean](required = false,
                   default = Some(false),
                   descr = "Skip the first unfilled partition range if some future partitions have been populated.")

    val stepDays: ScallopOption[Int] =
      opt[Int](required = false,
               descr = "Runs offline backfill in steps, step-days at a time. Default is 30 days",
               default = Option(30))

    val startPartitionOverride: ScallopOption[String] =
      opt[String](required = false,
                  descr = "Start date to compute offline backfill, " +
                    "this start date will override start partition specified in conf.")

    private val endDateInternal: ScallopOption[String] =
      opt[String](name = "end-date",
                  required = false,
                  descr = "End date to compute as of, start date is taken from conf.")

    val localTableMapping: Map[String, String] = propsLong[String](
      name = "local-table-mapping",
      keyName = "namespace.table",
      valueName = "path_to_local_input_file",
      descr = """Use this option to specify a list of table <> local input file mappings for running local
          |Chronon jobs. For example,
          |`--local-data-list ns_1.table_a=p1/p2/ta.csv ns_2.table_b=p3/tb.csv`
          |will load the two files into the specified tables `table_a` and `table_b` locally.
          |Once this option is used, the `--local-data-path` will be ignored.
          |""".stripMargin
    )
    val localDataPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder containing csv data to load from. You can refer to these in Sources to run the backfill.
            |The name of each file should be in the format namespace.table.csv. They can be referred to in the confs
            |as "namespace.table". When namespace is not specified we will default to 'default'. We can also
            |auto-convert ts columns encoded as readable strings in the format 'yyyy-MM-dd HH:mm:ss'',
            |into longs values expected by Chronon automatically.
            |""".stripMargin
      )
    val localWarehouseLocation: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some(System.getProperty("user.dir") + "/local_warehouse"),
        descr = "Directory to write locally loaded warehouse data into. This will contain unreadable parquet files"
      )

    lazy val sparkSession: SparkSession = buildSparkSession()

    def endDate(): String = endDateInternal.toOption.getOrElse(buildTableUtils().partitionSpec.now)

    def subcommandName(): String

    protected def isLocal: Boolean = localTableMapping.nonEmpty || localDataPath.isDefined

    protected def buildSparkSession(): SparkSession = {
      implicit val formats: Formats = DefaultFormats
      val yamlLoader = new Yaml()
      val additionalConfs = additionalConfPath.toOption
        .map(Source.fromFile)
        .map((src) =>
          try { src.mkString }
          finally { src.close })
        .map(yamlLoader.load(_).asInstanceOf[java.util.Map[String, Any]])
        .map((map) => Extraction.decompose(map.asScala.toMap))
        .map((v) => render(v))
        .map(compact)
        .map((str) => parse(str).extract[Map[String, String]])

      // We use the KryoSerializer for group bys and joins since we serialize the IRs.
      // But since staging query is fairly freeform, it's better to stick to the java serializer.
      val session =
        SparkSessionBuilder.build(
          subcommandName(),
          local = isLocal,
          localWarehouseLocation = localWarehouseLocation.toOption,
          enforceKryoSerializer = !subcommandName().contains("staging_query"),
          additionalConfig = additionalConfs
        )
      if (localTableMapping.nonEmpty) {
        localTableMapping.foreach { case (table, filePath) =>
          val file = new File(filePath)
          LocalDataLoader.loadDataFileAsTable(file, session, table)
        }
      } else if (localDataPath.isDefined) {
        val dir = new File(localDataPath())
        assert(dir.exists, s"Provided local data path: ${localDataPath()} doesn't exist")
        LocalDataLoader.loadDataRecursively(dir, session)
      }
      session
    }

    def buildTableUtils(): TableUtils = {
      TableUtils(sparkSession)
    }
  }

  trait LocalExportTableAbility {
    this: ScallopConf with OfflineSubcommand =>

    val localTableExportPath: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr =
          """Path to a folder for exporting all the tables generated during the run. This is only effective when local
            |input data is used: when either `local-table-mapping` or `local-data-path` is set. The name of the file
            |will be of format [<prefix>].<namespace>.<table_name>.<format>. For example: "default.test_table.csv" or
            |"local_prefix.some_namespace.another_table.parquet".
            |""".stripMargin
      )

    val localTableExportFormat: ScallopOption[String] =
      opt[String](
        required = false,
        default = Some("csv"),
        validate = (format: String) => LocalTableExporter.SupportedExportFormat.contains(format.toLowerCase),
        descr = "The table output format, supports csv(default), parquet, json."
      )

    val localTableExportPrefix: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = "The prefix to put in the exported file name."
      )

    protected def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter =
      new LocalTableExporter(tableUtils,
                             localTableExportPath(),
                             localTableExportFormat(),
                             localTableExportPrefix.toOption)

    def shouldExport(): Boolean = isLocal && localTableExportPath.isDefined

    def exportTableToLocal(namespaceAndTable: String, tableUtils: TableUtils): Unit = {
      val isLocal = localTableMapping.nonEmpty || localDataPath.isDefined
      val shouldExport = localTableExportPath.isDefined
      if (!isLocal || !shouldExport) {
        return
      }

      buildLocalTableExporter(tableUtils).exportTable(namespaceAndTable)
    }
  }

  trait ResultValidationAbility {
    this: ScallopConf with OfflineSubcommand =>

    val expectedResultTable: ScallopOption[String] =
      opt[String](
        required = false,
        default = None,
        descr = """The name of the table containing expected result of a job.
            |The table should have the exact schema of the output of the job""".stripMargin
      )

    def shouldPerformValidate(): Boolean = expectedResultTable.isDefined

    def validateResult(df: DataFrame, keys: Seq[String], tableUtils: TableUtils): Boolean = {
      val expectedDf = tableUtils.loadTable(expectedResultTable())
      val (_, _, metrics) = CompareBaseJob.compare(df, expectedDf, keys, tableUtils)
      val result = CompareJob.getConsolidatedData(metrics, tableUtils.partitionSpec)

      if (result.nonEmpty) {
        logger.info("[Validation] Failed. Please try exporting the result and investigate.")
        false
      } else {
        logger.info("[Validation] Success.")
        true
      }
    }
  }

  object JoinBackfill {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      val selectedJoinParts: ScallopOption[List[String]] =
        opt[List[String]](required = false, descr = "A list of join parts that require backfilling.")
      val useCachedLeft: ScallopOption[Boolean] =
        opt[Boolean](
          required = false,
          default = Some(false),
          descr = "Whether or not to use the cached bootstrap table as the source - used in parallelized join flow.")
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        tableUtils,
        !args.runFirstHole(),
        selectedJoinParts = args.selectedJoinParts.toOption
      )

      if (args.selectedJoinParts.isDefined) {
        join.computeJoinOpt(args.stepDays.toOption,
                            args.startPartitionOverride.toOption,
                            args.useCachedLeft.getOrElse(false))
        logger.info(
          s"Backfilling selected join parts: ${args.selectedJoinParts()} is complete. Skipping the final join. Exiting."
        )
        return
      }

      val df = join.computeJoin(args.stepDays.toOption, args.startPartitionOverride.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.joinConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val keys = CompareJob.getJoinKeys(args.joinConf, tableUtils)
        args.validateResult(df, keys, tableUtils)
      }

      df.show(numRows = 3, truncate = 0, vertical = true)
      logger.info(
        s"\nShowing three rows of output above.\nQuery table `${args.joinConf.metaData.outputTable}` for more.\n")
    }
  }

  object JoinBackfillLeft {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join-left")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_left_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        args.buildTableUtils(),
        !args.runFirstHole()
      )
      join.computeLeft(args.startPartitionOverride.toOption)
    }
  }

  object JoinBackfillFinal {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("join-final")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"join_final_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      args.buildTableUtils()
      val join = new Join(
        args.joinConf,
        args.endDate(),
        args.buildTableUtils(),
        !args.runFirstHole()
      )
      join.computeFinal(args.startPartitionOverride.toOption)
    }
  }

  object GroupByBackfill {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args
        extends Subcommand("group-by-backfill")
        with OfflineSubcommand
        with LocalExportTableAbility
        with ResultValidationAbility {
      lazy val groupByConf: api.GroupBy = parseConf[api.GroupBy](confPath())
      override def subcommandName(): String = s"groupBy_${groupByConf.metaData.name}_backfill"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      GroupBy.computeBackfill(
        args.groupByConf,
        args.endDate(),
        tableUtils,
        args.stepDays.toOption,
        args.startPartitionOverride.toOption,
        !args.runFirstHole()
      )

      if (args.shouldExport()) {
        args.exportTableToLocal(args.groupByConf.metaData.outputTable, tableUtils)
      }

      if (args.shouldPerformValidate()) {
        val df = tableUtils.loadTable(args.groupByConf.metaData.outputTable)
        args.validateResult(df, args.groupByConf.keys(tableUtils.partitionColumn).toSeq, tableUtils)
      }
    }
  }

  object LabelJoin {
    class Args extends Subcommand("label-join") with OfflineSubcommand with LocalExportTableAbility {
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"label_join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val labelJoin = new LabelJoin(
        args.joinConf,
        tableUtils,
        args.endDate()
      )
      labelJoin.computeLabelJoin(args.stepDays.toOption)

      if (args.shouldExport()) {
        args.exportTableToLocal(args.joinConf.metaData.outputLabelTable, tableUtils)
      }
    }
  }

  object Analyzer {
    class Args extends Subcommand("analyze") with OfflineSubcommand {
      val startDate: ScallopOption[String] =
        opt[String](required = false,
                    descr = "Finds skewed keys & time-distributions until a specified start date",
                    default = None)
      val skewKeyCount: ScallopOption[Int] =
        opt[Int](
          required = false,
          descr =
            "Finds the specified number of skewed keys. The larger this number is the more accurate the analysis will be.",
          default = Option(128)
        )
      val sample: ScallopOption[Double] =
        opt[Double](required = false,
                    descr = "Sampling ratio - what fraction of rows into incorporate into the skew key detection",
                    default = Option(0.1))
      val skewDetection: ScallopOption[Boolean] =
        opt[Boolean](
          required = false,
          descr =
            "finds skewed keys if true else will only output schema and exit. Skew detection will take longer time.",
          default = Some(false)
        )

      override def subcommandName() = "analyzer_util"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      new Analyzer(tableUtils,
                   args.confPath(),
                   args.startDate.getOrElse(tableUtils.partitionSpec.shiftBackFromNow(3)),
                   args.endDate(),
                   args.skewKeyCount(),
                   args.sample(),
                   args.skewDetection()).run
    }
  }

  object MetadataExport {
    class Args extends Subcommand("metadata-export") with OfflineSubcommand {
      val inputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path of config repo to export from")
      val outputRootPath: ScallopOption[String] =
        opt[String](required = true, descr = "Base path to write output metadata files to")
      override def subcommandName() = "metadata-export"
    }

    def run(args: Args): Unit = {
      MetadataExporter.run(args.inputRootPath(), args.outputRootPath())
    }
  }

  object StagingQueryBackfill {
    class Args extends Subcommand("staging-query-backfill") with OfflineSubcommand with LocalExportTableAbility {
      val enableAutoExpand: ScallopOption[Boolean] =
        opt[Boolean](required = false,
                     descr = "Auto expand hive table if new columns added in staging query",
                     default = Option(true))

      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](confPath())
      override def subcommandName(): String = s"staging_query_${stagingQueryConf.metaData.name}_backfill"
    }

    def run(args: Args): Unit = {
      val tableUtils = args.buildTableUtils()
      val stagingQueryJob = new StagingQuery(
        args.stagingQueryConf,
        args.endDate(),
        tableUtils
      )
      stagingQueryJob.computeStagingQuery(args.stepDays.toOption,
                                          args.enableAutoExpand.toOption,
                                          args.startPartitionOverride.toOption,
                                          !args.runFirstHole())

      if (args.shouldExport()) {
        args.exportTableToLocal(args.stagingQueryConf.metaData.outputTable, tableUtils)
      }
    }
  }

  object GroupByUploader {
    class Args extends Subcommand("group-by-upload") with OfflineSubcommand {
      override def subcommandName() = "group-by-upload"

      // jsonPercent
      val jsonPercent: ScallopOption[Int] =
        opt[Int](name = "json-percent",
                 required = false,
                 descr = "Percentage of json encoding to retain for debuggability",
                 default = Some(1))
    }

    def run(args: Args): Unit = {
      GroupByUpload.run(parseConf[api.GroupBy](args.confPath()),
                        args.endDate(),
                        Some(args.buildTableUtils()),
                        jsonPercent = args.jsonPercent.apply())
    }
  }

  object ConsistencyMetricsCompute {
    class Args extends Subcommand("consistency-metrics-compute") with OfflineSubcommand {
      override def subcommandName() = "consistency-metrics-compute"
    }

    def run(args: Args): Unit = {
      val joinConf = parseConf[api.Join](args.confPath())
      new ConsistencyJob(
        args.sparkSession,
        joinConf,
        args.endDate()
      ).buildConsistencyMetrics()
    }
  }

  object CompareJoinQuery {
    class Args extends Subcommand("compare-join-query") with OfflineSubcommand {
      val queryConf: ScallopOption[String] =
        opt[String](required = true, descr = "Conf to the Staging Query to compare with")
      val startDate: ScallopOption[String] =
        opt[String](required = false, descr = "Partition start date to compare the data from")

      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      lazy val stagingQueryConf: api.StagingQuery = parseConf[api.StagingQuery](queryConf())
      override def subcommandName(): String =
        s"compare_join_query_${joinConf.metaData.name}_${stagingQueryConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      assert(args.confPath().contains("/joins/"), "Conf should refer to the join path")
      assert(args.queryConf().contains("/staging_queries/"), "Compare path should refer to the staging query path")

      val tableUtils = args.buildTableUtils()
      new CompareJob(
        tableUtils,
        args.joinConf,
        args.stagingQueryConf,
        args.startDate.getOrElse(tableUtils.partitionSpec.at(System.currentTimeMillis())),
        args.endDate()
      ).run()
    }
  }

  // common arguments to all online commands
  trait OnlineSubcommand extends SharedSubCommandArgs { s: ScallopConf =>
    // this is `-Z` and not `-D` because sbt-pack plugin uses that for JAVA_OPTS
    val propsInner: Map[String, String] = props[String]('Z')
    val onlineJar: ScallopOption[String] =
      opt[String](required = true, descr = "Path to the jar contain the implementation of Online.Api class")
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

    // TODO: davidhan - remove this when we've migrated away from additional-conf-path
    val additionalConfPath: ScallopOption[String] =
      opt[String](required = false, descr = "Path to additional driver job configurations")

    // hashmap implements serializable
    def serializableProps: Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      propsInner.foreach { case (key, value) => map.update(key, value) }
      map.toMap
    }

    lazy private val gcpMap = Map(
      "GCP_PROJECT_ID" -> gcpProjectId.toOption.getOrElse(""),
      "GCP_BIGTABLE_INSTANCE_ID" -> gcpBigtableInstanceId.toOption.getOrElse("")
    )

    lazy val api: Api = isGcp.toOption match {
      case Some(true) => impl(serializableProps ++ gcpMap)
      case _          => impl(serializableProps)
    }

    def metaDataStore =
      new MetadataStore(api.genKvStore, MetadataDataset, timeoutMillis = 10000)

    def impl(props: Map[String, String]): Api = {
      val urls = Array(new File(onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
      val cls = cl.loadClass(onlineClass())
      val constructor = cls.getConstructors.apply(0)
      val onlineImpl = constructor.newInstance(props)
      onlineImpl.asInstanceOf[Api]
    }
  }

  object FetcherCli {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

    class Args extends Subcommand("fetch") with FetcherMain.FetcherArgs {}
    def run(args: Args): Unit = {
      FetcherMain.run(args)
    }
  }

  object MetadataUploader {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("metadata-upload") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Chronon config file or directory")
    }

    def run(args: Args): Unit = {
      val acceptedEndPoints = List(MetadataEndPoint.ConfByKeyEndPointName, MetadataEndPoint.NameByTeamEndPointName)
      val dirWalker = new MetadataDirWalker(args.confPath(), acceptedEndPoints, maybeConfType = args.confType.toOption)
      val kvMap: Map[String, Map[String, List[String]]] = dirWalker.run

      // trigger creates of the datasets before we proceed with writes
      acceptedEndPoints.foreach(e => args.metaDataStore.create(e))

      val putRequestsSeq: Seq[Future[scala.collection.Seq[Boolean]]] = kvMap.toSeq.map { case (endPoint, kvMap) =>
        args.metaDataStore.put(kvMap, endPoint)
      }
      val res = putRequestsSeq.flatMap(putRequests => Await.result(putRequests, 1.hour))
      logger.info(
        s"Uploaded Chronon Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object GroupByUploadToKVBulkLoad {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("groupby-upload-bulk-load") with OnlineSubcommand {
      // Expectation that run.py only sets confPath
      val confPath: ScallopOption[String] = opt[String](required = false, descr = "path to groupBy conf")

      val partitionString: ScallopOption[String] =
        opt[String](required = true, descr = "Partition string (in 'yyyy-MM-dd' format) that we are uploading")
    }

    def run(args: Args): Unit = {
      val groupByConf = parseConf[api.GroupBy](args.confPath())

      val offlineTable = groupByConf.metaData.uploadTable

      val groupByName = groupByConf.metaData.name

      logger.info(s"Triggering bulk load for GroupBy: ${groupByName} for partition: ${args
        .partitionString()} from table: ${offlineTable}")
      val kvStore = args.api.genKvStore
      val startTime = System.currentTimeMillis()

      try {
        // TODO: we may need to wrap this around TableUtils
        kvStore.bulkPut(offlineTable, groupByName, args.partitionString())
      } catch {
        case e: Exception =>
          logger.error(s"Failed to upload GroupBy: ${groupByName} for partition: ${args
                         .partitionString()} from table: $offlineTable",
                       e)
          throw e
      }
      logger.info(s"Uploaded GroupByUpload data to KV store for GroupBy: ${groupByName}; partition: ${args
        .partitionString()} in ${(System.currentTimeMillis() - startTime) / 1000} seconds")
    }
  }

  object LogFlattener {
    class Args extends Subcommand("log-flattener") with OfflineSubcommand {
      val logTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with partitioned raw logs")

      val schemaTable: ScallopOption[String] =
        opt[String](required = true, descr = "Hive table with mapping from schema_hash to schema_value_last")
      lazy val joinConf: api.Join = parseConf[api.Join](confPath())
      override def subcommandName(): String = s"log_flattener_join_${joinConf.metaData.name}"
    }

    def run(args: Args): Unit = {
      val spark = args.sparkSession
      val logFlattenerJob = new LogFlattenerJob(
        spark,
        args.joinConf,
        args.endDate(),
        args.logTable(),
        args.schemaTable(),
        Some(args.stepDays())
      )
      logFlattenerJob.buildLogTable()
    }
  }

  object GroupByStreaming {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
      TopicChecker.topicShouldExist(topic, host)
      session.streams.addListener(new StreamingQueryListener() {
        override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
          logger.info("Query started: " + queryStarted.id)
        }
        override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
          logger.info("Query terminated: " + queryTerminated.id)
        }
        override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
          logger.info("Query made progress: " + queryProgress.progress)
        }
      })
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
        .selectExpr("value")
    }

    class Args extends Subcommand("group-by-streaming") with OnlineSubcommand {
      @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val DEFAULT_LAG_MILLIS = 2000 // 2seconds
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = false, descr = "host:port of a kafka bootstrap server")
      val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                            default = Some(false),
                                                            descr =
                                                              "flag - to ignore writing to the underlying kv store")
      val debug: ScallopOption[Boolean] = opt[Boolean](
        required = false,
        default = Some(false),
        descr = "Prints details of data flowing through the streaming job, skip writing to kv store")
      val lagMillis: ScallopOption[Int] = opt[Int](
        required = false,
        default = Some(DEFAULT_LAG_MILLIS),
        descr = "Lag time for chaining, before fetching upstream join results, in milliseconds. Default 2 seconds"
      )
      def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
        ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)
    }

    def findFile(path: String): Option[String] = {
      val tail = path.split("/").last
      val possiblePaths = Seq(path, tail, SparkFiles.get(tail))
      val statuses = possiblePaths.map(p => p -> new File(p).exists())

      val messages = statuses.map { case (file, present) =>
        val suffix = if (present) {
          val fileSize = Files.size(Paths.get(file))
          s"exists ${FileUtils.byteCountToDisplaySize(fileSize)}"
        } else {
          "is not found"
        }
        s"$file $suffix"
      }
      logger.info(s"File Statuses:\n  ${messages.mkString("\n  ")}")
      statuses.find(_._2 == true).map(_._1)
    }

    def run(args: Args): Unit = {
      // session needs to be initialized before we can call find file.
      implicit val session: SparkSession = SparkSessionBuilder.buildStreaming(args.debug())

      val confFile = findFile(args.confPath())
      val groupByConf = confFile
        .map(ThriftJsonCodec.fromJsonFile[api.GroupBy](_, check = false))
        .getOrElse(args.metaDataStore.getConf[api.GroupBy](args.confPath()).get)

      val onlineJar = findFile(args.onlineJar())
      if (args.debug())
        onlineJar.foreach(session.sparkContext.addJar)
      implicit val apiImpl = args.impl(args.serializableProps)
      val query = if (groupByConf.streamingSource.get.isSetJoinSource) {
        new JoinSourceRunner(groupByConf,
                             args.serializableProps,
                             args.debug(),
                             args.lagMillis.getOrElse(2000)).chainedStreamingQuery.start()
      } else {
        val streamingSource = groupByConf.streamingSource
        assert(streamingSource.isDefined,
               "There is no valid streaming source - with a valid topic, and endDate < today")
        lazy val host = streamingSource.get.topicTokens.get("host")
        lazy val port = streamingSource.get.topicTokens.get("port")
        if (!args.kafkaBootstrap.isDefined)
          assert(
            host.isDefined && port.isDefined,
            "Either specify a kafkaBootstrap url or provide host and port in your topic definition as topic/host=host/port=port")
        val inputStream: DataFrame =
          dataStream(session, args.kafkaBootstrap.getOrElse(s"${host.get}:${port.get}"), streamingSource.get.cleanTopic)
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl(args.serializableProps), args.debug()).run()
      }
      query.awaitTermination()
    }
  }

  object CreateSummaryDataset {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("create-summary-dataset") with OnlineSubcommand

    def run(args: Args): Unit = {
      logger.info(s"Creating table '${Constants.TiledSummaryDataset}'")
      val store = args.api.genKvStore
      val props = Map("is-time-sorted" -> "true")
      store.create(Constants.TiledSummaryDataset, props)
    }
  }

  object SummarizeAndUpload {
    @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    class Args extends Subcommand("summarize-and-upload") with OnlineSubcommand {

      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Name of the conf to summarize - joins/team/file.variable")

      //TODO: we should pull conf from conf path and figure out table name from the conf instead
      val parquetPath: ScallopOption[String] =
        opt[String](required = true, descr = "Location of the parquet containing the data to summarize")

      val timeColumn: ScallopOption[String] =
        opt[String](required = false, descr = "The column in the dataset which tracks the time")
    }

    // drift for all the conf-s go into same online table, but different offline table
    def run(args: Args): Unit = {
      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName("ParquetReader")
        .config("spark.master", sys.env.getOrElse("SPARK_MASTER_URL", "local"))
        .config("spark.jars", args.onlineJar())
        .getOrCreate()
      implicit val tableUtils: TableUtils = TableUtils(sparkSession)
      logger.info("Running Summarizer")
      val confPath = args.confPath()
      val summarizer = new Summarizer(args.api, confPath, timeColumn = args.timeColumn.toOption)
      try {
        val df = sparkSession.read.parquet(args.parquetPath())
        val (result, summaryExprs) = summarizer.computeSummaryDf(df)
        val packer = new SummaryPacker(confPath, summaryExprs, summarizer.tileSize, summarizer.sliceColumns)
        val (packed, _) = packer.packSummaryDf(result)

        val uploader = new SummaryUploader(packed, args.api)
        uploader.run()
      } catch {
        case e: Exception =>
          logger.error(s"Failed to summarize and upload data for conf: $confPath", e)
          throw e
      }
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    object JoinBackFillArgs extends JoinBackfill.Args
    addSubcommand(JoinBackFillArgs)
    object LogFlattenerArgs extends LogFlattener.Args
    addSubcommand(LogFlattenerArgs)
    object ConsistencyMetricsArgs extends ConsistencyMetricsCompute.Args
    addSubcommand(ConsistencyMetricsArgs)
    object GroupByBackfillArgs extends GroupByBackfill.Args
    addSubcommand(GroupByBackfillArgs)
    object StagingQueryBackfillArgs extends StagingQueryBackfill.Args
    addSubcommand(StagingQueryBackfillArgs)
    object GroupByUploadArgs extends GroupByUploader.Args
    addSubcommand(GroupByUploadArgs)
    object FetcherCliArgs extends FetcherCli.Args
    addSubcommand(FetcherCliArgs)
    object MetadataUploaderArgs extends MetadataUploader.Args
    addSubcommand(MetadataUploaderArgs)
    object GroupByUploadToKVBulkLoadArgs extends GroupByUploadToKVBulkLoad.Args
    addSubcommand(GroupByUploadToKVBulkLoadArgs)
    object GroupByStreamingArgs extends GroupByStreaming.Args
    addSubcommand(GroupByStreamingArgs)
    object AnalyzerArgs extends Analyzer.Args
    addSubcommand(AnalyzerArgs)
    object CompareJoinQueryArgs extends CompareJoinQuery.Args
    addSubcommand(CompareJoinQueryArgs)
    object MetadataExportArgs extends MetadataExport.Args
    addSubcommand(MetadataExportArgs)
    object JoinBackfillLeftArgs extends JoinBackfillLeft.Args
    addSubcommand(JoinBackfillLeftArgs)
    object JoinBackfillFinalArgs extends JoinBackfillFinal.Args
    addSubcommand(JoinBackfillFinalArgs)
    object LabelJoinArgs extends LabelJoin.Args
    addSubcommand(LabelJoinArgs)
    object CreateStatsTableArgs extends CreateSummaryDataset.Args
    addSubcommand(CreateStatsTableArgs)
    object SummarizeAndUploadArgs extends SummarizeAndUpload.Args
    addSubcommand(SummarizeAndUploadArgs)
    requireSubcommand()
    verify()
  }

  def onlineBuilder(userConf: Map[String, String], onlineJar: String, onlineClass: String): Api = {
    val urls = Array(new File(onlineJar).toURI.toURL)
    val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(userConf)
    onlineImpl.asInstanceOf[Api]
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    var shouldExit = true
    args.subcommand match {
      case Some(x) =>
        x match {
          case args.JoinBackFillArgs         => JoinBackfill.run(args.JoinBackFillArgs)
          case args.GroupByBackfillArgs      => GroupByBackfill.run(args.GroupByBackfillArgs)
          case args.StagingQueryBackfillArgs => StagingQueryBackfill.run(args.StagingQueryBackfillArgs)
          case args.GroupByUploadArgs        => GroupByUploader.run(args.GroupByUploadArgs)
          case args.GroupByStreamingArgs =>
            shouldExit = false
            GroupByStreaming.run(args.GroupByStreamingArgs)

          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.GroupByUploadToKVBulkLoadArgs =>
            GroupByUploadToKVBulkLoad.run(args.GroupByUploadToKVBulkLoadArgs)
          case args.FetcherCliArgs         => FetcherCli.run(args.FetcherCliArgs)
          case args.LogFlattenerArgs       => LogFlattener.run(args.LogFlattenerArgs)
          case args.ConsistencyMetricsArgs => ConsistencyMetricsCompute.run(args.ConsistencyMetricsArgs)
          case args.CompareJoinQueryArgs   => CompareJoinQuery.run(args.CompareJoinQueryArgs)
          case args.AnalyzerArgs           => Analyzer.run(args.AnalyzerArgs)
          case args.MetadataExportArgs     => MetadataExport.run(args.MetadataExportArgs)
          case args.LabelJoinArgs          => LabelJoin.run(args.LabelJoinArgs)
          case args.JoinBackfillLeftArgs   => JoinBackfillLeft.run(args.JoinBackfillLeftArgs)
          case args.JoinBackfillFinalArgs  => JoinBackfillFinal.run(args.JoinBackfillFinalArgs)
          case args.CreateStatsTableArgs   => CreateSummaryDataset.run(args.CreateStatsTableArgs)
          case args.SummarizeAndUploadArgs => SummarizeAndUpload.run(args.SummarizeAndUploadArgs)
          case _                           => logger.info(s"Unknown subcommand: $x")
        }
      case None => logger.info("specify a subcommand please")
    }
    if (shouldExit) {
      System.exit(0)
    }
  }
}
