package ai.chronon.orchestration.test.utils

import ai.chronon.api.{Job, JobInfo, JobStatusType, TableDependency, TableInfo, TimeUnit, Window}
import ai.chronon.orchestration.persistence.NodeTableDependency
import ai.chronon.orchestration.temporal.NodeName

object TestUtils {

  // Helper method to create a TableDependency for testing
  def createTestTableDependency(
      tableName: String,
      partitionColumn: Option[String] = None,
      startOffsetDays: Option[Int] = Some(0),
      endOffsetDays: Option[Int] = Some(0)
  ): TableDependency = {
    val tableInfo = new TableInfo().setTable(tableName)
    partitionColumn.foreach(tableInfo.setPartitionColumn)

    // Create a window for partition interval
    val partitionWindow = new Window()
      .setLength(1)
      .setTimeUnit(TimeUnit.DAYS)
    tableInfo.setPartitionInterval(partitionWindow)

    // Create the table dependency
    val tableDependency = new TableDependency().setTableInfo(tableInfo)

    // Add start offset
    startOffsetDays.foreach { days =>
      val offsetWindow = new Window()
        .setLength(days)
        .setTimeUnit(TimeUnit.DAYS)
      tableDependency.setStartOffset(offsetWindow)
    }

    // Add end offset
    endOffsetDays.foreach { days =>
      val offsetWindow = new Window()
        .setLength(days)
        .setTimeUnit(TimeUnit.DAYS)
      tableDependency.setEndOffset(offsetWindow)
    }

    tableDependency
  }

  // Helper method to create a NodeTableDependency for testing
  def createTestNodeTableDependency(parent: String,
                                    child: String,
                                    tableName: String,
                                    startOffsetDays: Option[Int] = Some(0),
                                    endOffsetDays: Option[Int] = Some(0)): NodeTableDependency = {
    NodeTableDependency(
      NodeName(parent),
      NodeName(child),
      createTestTableDependency(tableName, Some("dt"), startOffsetDays, endOffsetDays)
    )
  }

  def createTestJob(jobId: String): Job = {
    val jobInfo = new JobInfo()
    jobInfo.setJobId(jobId)
    jobInfo.setCurrentStatus(JobStatusType.PENDING)

    val job = new Job()
    job.setJobInfo(jobInfo)
    job
  }

}
