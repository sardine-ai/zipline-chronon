package ai.chronon.api.test

import ai.chronon.api._
import ai.chronon.api.planner.DependencyResolver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencyResolverSpec extends AnyFlatSpec with Matchers {

  // Common test objects
  implicit val partitionSpec: PartitionSpec = PartitionSpec.daily

  // Test add method
  "DependencyResolver.add" should "add two windows with the same time unit" in {
    val window1 = new Window(2, TimeUnit.DAYS)
    val window2 = new Window(3, TimeUnit.DAYS)

    val result = DependencyResolver.add(window1, window2)

    result.length shouldBe 5
    result.timeUnit shouldBe TimeUnit.DAYS
  }

  it should "use the first window's time unit when adding windows with different time units" in {
    val window1 = new Window(2, TimeUnit.DAYS)
    val window2 = new Window(12, TimeUnit.HOURS)

    val result = DependencyResolver.add(window1, window2)

    result.length shouldBe 2 + 1 // 2 days + 12 hours = 2 days + 0.5 days rounded up = 3 days
    result.timeUnit shouldBe TimeUnit.DAYS
  }

  it should "return the second window when the first window is null" in {
    val window1 = null
    val window2 = new Window(3, TimeUnit.DAYS)

    val result = DependencyResolver.add(window1, window2)

    result shouldBe window2
  }

  it should "return the first window when the second window is null" in {
    val window1 = new Window(2, TimeUnit.DAYS)
    val window2 = null

    val result = DependencyResolver.add(window1, window2)

    result shouldBe window1
  }

  // Test tableDependency method
  "DependencyResolver.tableDependency" should "create a table dependency with correct properties" in {
    // Create a test source
    val source = new Source()
    val eventSource = new EventSource()
    eventSource.setTable("test_table")

    val query = new Query()
    query.setStartPartition("2023-01-01")
    query.setEndPartition("2023-01-10")
    eventSource.setQuery(query)

    source.setEvents(eventSource)

    val startOffset = new Window(1, TimeUnit.DAYS)
    val endOffset = new Window(0, TimeUnit.DAYS)

    val dependency = DependencyResolver.tableDependency(source, startOffset, endOffset)

    dependency.getStartOffset shouldBe startOffset
    dependency.getEndOffset shouldBe endOffset
    dependency.getStartCutOff shouldBe "2023-01-01"
    dependency.getEndCutOff shouldBe "2023-01-10"
    dependency.getTableInfo.getTable shouldBe "test_table"
    dependency.getTableInfo.isCumulative shouldBe false
  }

  it should "handle mutation tables when isMutation is true" in {
    // Create a test source with a mutation table
    val source = new Source()
    val entitySource = new EntitySource()
    entitySource.setSnapshotTable("snapshot_table")
    entitySource.setMutationTable("mutation_table")

    val query = new Query()
    query.setStartPartition("2023-01-01")
    query.setEndPartition("2023-01-10")
    entitySource.setQuery(query)

    source.setEntities(entitySource)

    val dependency = DependencyResolver.tableDependency(source, null, null, isMutation = true)

    dependency.getTableInfo.getTable shouldBe "mutation_table"
  }

  it should "throw an assertion error when isMutation is true but no mutation table exists" in {
    // Create a test source without a mutation table
    val source = new Source()
    val entitySource = new EntitySource()
    entitySource.setSnapshotTable("snapshot_table")

    val query = new Query()
    query.setStartPartition("2023-01-01")
    query.setEndPartition("2023-01-10")
    entitySource.setQuery(query)

    source.setEntities(entitySource)

    assertThrows[AssertionError] {
      DependencyResolver.tableDependency(source, null, null, isMutation = true)
    }
  }

  // Test computeInputRange method
  "DependencyResolver.computeInputRange" should "compute the correct input range for non-cumulative sources when cutoff is after query end partition" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(false)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets
    tableDep.setStartOffset(new Window(1, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(0, TimeUnit.DAYS))

    // Set cutoffs
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-31")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe true
    result.get.start shouldBe "2023-01-09" // 2023-01-10 - 1 day, but constrained by startCutOff
    result.get.end shouldBe "2023-01-20" // 2023-01-20 - 0 days, constrained by endCutOff
  }

  "DependencyResolver.computeInputRange" should "compute the correct input range for non-cumulative sources when cutoff is before query end partition" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(false)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets
    tableDep.setStartOffset(new Window(0, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(1, TimeUnit.DAYS))

    // Set cutoffs
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-15")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe true
    result.get.start shouldBe "2023-01-10" // 2023-01-10 - 0 day, but constrained by startCutOff
    result.get.end shouldBe "2023-01-15" // 2023-01-15 - 1 days, constrained by endCutOff
  }

  it should "return None when the computed start partition is after the end partition based on offsets" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(false)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets to create an invalid range
    tableDep.setStartOffset(new Window(0, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(15, TimeUnit.DAYS))

    // Set cutoffs
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-30")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe false
  }

  it should "return None when the computed start partition is after the end partition based on cutoffs" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(false)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets
    tableDep.setStartOffset(new Window(0, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(0, TimeUnit.DAYS))

    // Set cutoffs to create an invalid range
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-09")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe false
  }

  it should "compute the correct input range for cumulative sources when cutoff is after query end partition" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(true)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set end cutoff
    tableDep.setEndCutOff("2023-01-31")
    tableDep.setEndOffset(new Window(1, TimeUnit.DAYS))

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe true
    // For cumulative sources, we always compute the latest possible partition
    result.get.start shouldBe "2023-01-30" // 2023-01-31 - 1 day
    result.get.end shouldBe "2023-01-30" // Same as start for cumulative sources
  }

  it should "compute the correct input range for cumulative sources when cutoff is before query end partition" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(true)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set end cutoff
    tableDep.setEndCutOff("2023-01-15")
    tableDep.setEndOffset(new Window(1, TimeUnit.DAYS))

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe true
    // For cumulative sources, we always compute the latest possible partition
    result.get.start shouldBe "2023-01-14" // 2023-01-15 - 1 day
    result.get.end shouldBe "2023-01-14" // Same as start for cumulative sources
  }

  it should "return None for cumulative sources when the computed start partition is after the end partition based on offsets" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(true)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets to create an invalid range
    tableDep.setStartOffset(new Window(0, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(15, TimeUnit.DAYS))

    // Set cutoffs
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-30")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe false
  }

  it should "return None for cumulative sources when the computed start partition is after the end partition based on cutoffs" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(true)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    // Set offsets
    tableDep.setStartOffset(new Window(0, TimeUnit.DAYS))
    tableDep.setEndOffset(new Window(0, TimeUnit.DAYS))

    // Set cutoffs to create an invalid range
    tableDep.setStartCutOff("2023-01-01")
    tableDep.setEndCutOff("2023-01-09")

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe false
  }

  it should "use the current date as the end cutoff for cumulative sources if none is specified" in {
    val queryRange = PartitionRange("2023-01-10", "2023-01-20")

    val tableDep = new TableDependency()
    val tableInfo = new TableInfo()
    tableInfo.setIsCumulative(true)
    tableInfo.setTable("test_table")
    tableDep.setTableInfo(tableInfo)

    val today = partitionSpec.now
    tableDep.setEndOffset(new Window(1, TimeUnit.DAYS))

    val result = DependencyResolver.computeInputRange(queryRange, tableDep)

    result.isDefined shouldBe true
    // For cumulative sources, we always compute the latest possible partition
    result.get.start shouldBe partitionSpec.minus(today, new Window(1, TimeUnit.DAYS))
    result.get.end shouldBe partitionSpec.minus(today, new Window(1, TimeUnit.DAYS))
  }

  // Test getMissingSteps method
  "DependencyResolver.getMissingSteps" should "return the correct missing steps" in {
    val requiredRange = PartitionRange("2023-01-01", "2023-01-10")
    val existingPartitions = Seq("2023-01-01", "2023-01-02", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10")

    val missingSteps = DependencyResolver.getMissingSteps(requiredRange, existingPartitions)

    missingSteps.size shouldBe 4 // Missing 2023-01-03, 2023-01-04, 2023-01-07, 2023-01-08
    missingSteps.map(_.start) should contain allOf ("2023-01-03", "2023-01-04", "2023-01-07", "2023-01-08")
    missingSteps.map(_.end) should contain allOf ("2023-01-03", "2023-01-04", "2023-01-07", "2023-01-08")
  }

  it should "collapse consecutive missing partitions into ranges with the specified step days" in {
    val requiredRange = PartitionRange("2023-01-01", "2023-01-10")
    val existingPartitions = Seq("2023-01-01", "2023-01-06", "2023-01-10")

    // Using step days = 2
    val missingSteps = DependencyResolver.getMissingSteps(requiredRange, existingPartitions, stepDays = 2)

    missingSteps.size shouldBe 4 // Should return 4 steps: 2023-01-02+2023-01-03, 2023-01-04+2023-01-05, 2023-01-07+2023-01-08, 2023-01-09
    missingSteps.map(_.start) should contain allOf ("2023-01-02", "2023-01-04", "2023-01-07", "2023-01-09")
    missingSteps.map(_.end) should contain allOf ("2023-01-03", "2023-01-05", "2023-01-09")
  }

  it should "return an empty sequence when there are no missing steps" in {
    val requiredRange = PartitionRange("2023-01-01", "2023-01-05")
    val existingPartitions = Seq("2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05")

    val missingSteps = DependencyResolver.getMissingSteps(requiredRange, existingPartitions)

    missingSteps shouldBe empty
  }
}
