package ai.chronon.api.planner
import ai.chronon.api.Extensions.{MetadataOps, StringOps, WindowUtils}
import ai.chronon.api.ScalaJavaConversions.{JListOps, MapOps}
import ai.chronon.api.{
  ConfigType,
  Constants,
  ExecutionInfo,
  MetaData,
  PartitionSpec,
  TableDependency,
  TableInfo,
  ThriftJsonCodec
}
import ai.chronon.api
import ai.chronon.api.Constants.{getClass, _}
import ai.chronon.api.Extensions._
import ai.chronon.api.thrift.TBase
import com.google.gson.Gson
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileReader}
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag
import scala.util.Try
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
