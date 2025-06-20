package ai.chronon.api.planner
import ai.chronon.api.{ExecutionInfo, MetaData, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Constants._
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.JListOps

import java.util
import scala.collection.Seq

object MetaDataUtils {

  def layer(baseMetadata: MetaData,
            modeName: String,
            nodeName: String,
            tableDependencies: Seq[TableDependency],
            stepDays: Option[Int] = None)(implicit partitionSpec: PartitionSpec): MetaData = {

    val copy = baseMetadata.deepCopy()
    val newName = nodeName
    copy.setName(newName)

    val baseExecutionInfo = Option(copy.executionInfo).getOrElse(new ExecutionInfo())
    val mergedExecutionInfo = mergeModeConfAndEnv(baseExecutionInfo, modeName)
    copy.setExecutionInfo(mergedExecutionInfo)

    // if stepDays is passed in respect it, otherwise use what's already there, otherwise set it to 1.
    if (stepDays.nonEmpty) {
      copy.executionInfo.setStepDays(stepDays.get)
    } else if (!copy.executionInfo.isSetStepDays) {
      copy.executionInfo.setStepDays(1)
    }

    // legacy output table and new style should match:
    // align metadata.outputTable == metadata.executionInfo.outputTableInfo.table
    if (copy.executionInfo.outputTableInfo == null) {
      copy.executionInfo.setOutputTableInfo(new TableInfo())
    }
    // fully qualified: namespace + outputTable
    copy.executionInfo.outputTableInfo
      .setTable(copy.outputTable)
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(WindowUtils.hours(partitionSpec.spanMillis))

    // set table dependencies
    copy.executionInfo.setTableDependencies(tableDependencies.toJava)

    copy
  }

  // merge common + mode confs and envs, discard others and return a simpler / leaner execution info
  private def mergeModeConfAndEnv(executionInfo: ExecutionInfo, mode: String): ExecutionInfo = {

    val result = executionInfo.deepCopy()

    if (executionInfo.conf != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.conf.common != null) merged.putAll(executionInfo.conf.common)

      if (executionInfo.conf.modeConfigs != null) {
        val modeConf = executionInfo.conf.modeConfigs.get(mode)
        if (modeConf != null) merged.putAll(modeConf)
      }

      result.conf.setCommon(merged)
      result.conf.unsetModeConfigs()
    }

    if (executionInfo.env != null) {
      val merged = new util.HashMap[String, String]()

      if (executionInfo.env.common != null) merged.putAll(executionInfo.env.common)

      if (executionInfo.env.modeEnvironments != null) {
        val modeEnv = executionInfo.env.modeEnvironments.get(mode)
        if (modeEnv != null) merged.putAll(modeEnv)
      }

      result.env.setCommon(merged)
      result.env.unsetModeEnvironments()
    }

    result
  }

}
