package ai.chronon.spark.batch

import ai.chronon.api
import ai.chronon.api.planner.NodeRunner
import ai.chronon.api.{MetaData, PartitionRange, PartitionSpec, ThriftJsonCodec}
import ai.chronon.planner.{MonolithJoinNode, NodeContent, StagingQueryNode}
import ai.chronon.spark.Join
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

class BatchNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {

  this: ScallopConf =>

  val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to node configuration file")
  val startDs: ScallopOption[String] = opt[String](
    required = true,
    descr = "Start date string in format yyyy-MM-dd, used for partitioning"
  )
  val endDs: ScallopOption[String] = opt[String](
    required = true,
    descr = "End date string in format yyyy-MM-dd, used for partitioning"
  )

  val confType: ScallopOption[String] = choice(
    required = true,
    choices = Seq("join", "staging_query"),
    descr = "Type of configuration, e.g., 'join', 'staging_query', etc."
  )

  val confMode: ScallopOption[String] = opt[String](
    required = true,
    validate = Set("batch", "streaming").contains(_),
    descr = "Mode of execution, e.g., 'batch', 'streaming', etc."
  )

  verify()
}

object BatchNodeRunner extends NodeRunner {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private def createTableUtils(name: String): TableUtils = {
    val spark = SparkSessionBuilder.build(s"batch-node-runner-${name}")
    TableUtils(spark)
  }

  private[batch] def run(metadata: MetaData, conf: NodeContent, range: PartitionRange, tableUtils: TableUtils): Unit = {
    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN =>
        runMonolithJoin(metadata, conf.getMonolithJoin, range, tableUtils)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported NodeContent type: ${conf.getSetField}")
    }
  }

  private def runMonolithJoin(metadata: MetaData,
                              monolithJoin: MonolithJoinNode,
                              range: PartitionRange,
                              tableUtils: TableUtils): Unit = {
    require(monolithJoin.isSetJoin, "MonolithJoinNode must have a join set")

    val joinConf = monolithJoin.join
    val joinName = metadata.name
    val skewFreeMode = tableUtils.sparkSession.conf
      .get("spark.chronon.join.backfill.mode.skewFree", "false")
      .toBoolean

    logger.info(s"Running join backfill for '$joinName' with skewFree mode: $skewFreeMode")
    logger.info(s"Processing range: [${range.start}, ${range.end}]")

    if (skewFreeMode) {
      UnionJoin.computeJoinAndSave(joinConf, range)(tableUtils)
      logger.info(s"Successfully wrote range: $range")
    } else {
      val join = new Join(joinConf, range.end, tableUtils)
      val df = join.computeJoin(overrideStartPartition = Option(range.start))

      df.show(numRows = 3, truncate = 0, vertical = true)
      logger.info(s"\nShowing three rows of output above.\nQuery table '$joinName' for more.\n")
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit = {
    require(range.isDefined, "Partition range must be defined for batch node runner")
    run(metadata, conf, range.get, createTableUtils(metadata.name))
  }

  private[batch] def loadNodeContent(confPath: String, confType: String): (MetaData, NodeContent) = {
    val nodeContent = new NodeContent()

    confType match {
      case "join" =>
        val joinObj = ThriftJsonCodec.fromJsonFile[api.Join](confPath, check = false)
        val monolithJoin = new MonolithJoinNode()
        monolithJoin.setJoin(joinObj)
        nodeContent.setMonolithJoin(monolithJoin)
        (joinObj.metaData, nodeContent)

      case "staging_query" =>
        val stagingQueryObj = ThriftJsonCodec.fromJsonFile[api.StagingQuery](confPath, check = false)
        val stagingQueryNode = new StagingQueryNode()
        stagingQueryNode.setStagingQuery(stagingQueryObj)
        nodeContent.setStagingQuery(stagingQueryNode)
        (stagingQueryObj.metaData, nodeContent)

      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported config type: $unsupported")
    }
  }

  def runFromArgs(confPath: String, startDs: String, endDs: String, confType: String): Try[Unit] = {
    Try {
      val range = PartitionRange(startDs, endDs)(PartitionSpec.daily)
      val (metadata, nodeContent) = loadNodeContent(confPath, confType)

      logger.info(s"Starting batch node runner for '${metadata.name}'")
      run(metadata, nodeContent, Option(range))
      logger.info(s"Successfully completed batch node runner for '${metadata.name}'")
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val batchArgs = new BatchNodeRunnerArgs(args)

      runFromArgs(batchArgs.confPath(), batchArgs.startDs(), batchArgs.endDs(), batchArgs.confType()) match {
        case Success(_) =>
          logger.info("Batch node runner completed successfully")
        case Failure(exception) =>
          logger.error("Batch node runner failed", exception)
          System.exit(1)
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to parse arguments or initialize runner", e)
        System.exit(1)
    }
  }
}
