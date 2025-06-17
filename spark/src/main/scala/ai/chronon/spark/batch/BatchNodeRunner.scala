package ai.chronon.spark.batch

import ai.chronon.api.{MetaData, PartitionRange}
import ai.chronon.api.planner.NodeRunner
import ai.chronon.planner.NodeContent
import ai.chronon.spark.Join
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import org.slf4j.{Logger, LoggerFactory}

object BatchNodeRunner extends NodeRunner {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private def tableUtils(name: String): TableUtils = {
    val spark = SparkSessionBuilder.build(s"batch-node-runner-${name}")
    TableUtils(spark)
  }

  private[batch] def run(metadata: MetaData, conf: NodeContent, range: PartitionRange, tableUtils: TableUtils): Unit = {
    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN => {
        val monolithJoin = conf.getMonolithJoin
        require(monolithJoin.isSetJoin, "MonolithJoinNode must have a join set")
        val joinConf = monolithJoin.join
        val joinName = metadata.name
        val skewFreeMode =
          tableUtils.sparkSession.conf.get("spark.chronon.join.backfill.mode.skewFree", "false").toBoolean
        logger.info(s" >>> Running join backfill with skewFree mode set to: ${skewFreeMode} <<< ")
        if (skewFreeMode) {

          logger.info(s"Filling partitions for join:$joinName, partitions:[${range.start}, ${range.end}]")

          logger.info(s"Processing range $range)")
          UnionJoin.computeJoinAndSave(joinConf, range)(tableUtils)
          logger.info(s"Wrote range $range)")

        } else {

          val join = new Join(
            joinConf,
            range.end,
            tableUtils
          )

          val df = join.computeJoin(overrideStartPartition = Option(range.start))

          df.show(numRows = 3, truncate = 0, vertical = true)
          logger.info(s"\nShowing three rows of output above.\nQuery table `${joinName}` for more.\n")
        }

      }
      case _ => throw new UnsupportedOperationException("Unsupported NodeContent type: " + conf.getClass.getName)
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, range: Option[PartitionRange]): Unit = {
    require(range.isDefined, "Partition range must be defined for batch node runner")
    run(metadata, conf, range.get, tableUtils(metadata.name))
  }
}
