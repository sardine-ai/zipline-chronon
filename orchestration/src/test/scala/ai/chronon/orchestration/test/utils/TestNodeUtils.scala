package ai.chronon.orchestration.test.utils

import ai.chronon.orchestration.DummyNode

import java.util

object TestNodeUtils {

  // Create simple dependency graph:
  //          main
  //        /      \
  //     dep1      dep2
  def getSimpleNode: DummyNode = {
    val depNode1 = new DummyNode().setName("dep1") // Leaf node 1
    val depNode2 = new DummyNode().setName("dep2") // Leaf node 2
    new DummyNode()
      .setName("main") // Root node
      .setDependencies(util.Arrays.asList(depNode1, depNode2)) // Main node depends on both leaf nodes
  }

  // Create complex dependency graph:
  //                    Derivation
  //                        |
  //                      Join
  //                   /        \
  //             GroupBy1     GroupBy2
  //                |            |
  //          StagingQuery1  StagingQuery2
  def getComplexNode: DummyNode = {
    // Create base level queries (leaf nodes)
    val stagingQuery1 = new DummyNode().setName("StagingQuery1") // e.g., "SELECT * FROM raw_events"
    val stagingQuery2 = new DummyNode().setName("StagingQuery2") // e.g., "SELECT * FROM raw_metrics"

    // Create aggregation level nodes
    val groupBy1 = new DummyNode()
      .setName("GroupBy1") // e.g., "GROUP BY event_type, timestamp"
      .setDependencies(util.Arrays.asList(stagingQuery1))

    val groupBy2 = new DummyNode()
      .setName("GroupBy2") // e.g., "GROUP BY metric_name, interval"
      .setDependencies(util.Arrays.asList(stagingQuery2))

    // Create join level
    val join = new DummyNode()
      .setName("Join") // e.g., "JOIN grouped_events WITH grouped_metrics"
      .setDependencies(util.Arrays.asList(groupBy1, groupBy2))

    // Return final derivation node
    new DummyNode()
      .setName("Derivation") // e.g., "CALCULATE final_metrics"
      .setDependencies(util.Arrays.asList(join))
  }
}
