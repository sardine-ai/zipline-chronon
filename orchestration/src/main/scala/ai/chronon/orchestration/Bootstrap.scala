package ai.chronon.orchestration

import ai.chronon.api
import ai.chronon.online.PartitionRange

class Bootstrap {

  // Given a Join and a requested range, return a map of JoinPart -> PartitionRange that represents the bootstrap range
  // for that JoinPart. This is used as an input to the rest of the chunking/stepping logic that orchestrator is responsible for.
  // The result of all that is used when issuing:
  // 1. joinPartJobs -- we won't issue jobs for the covered ranges for each joinPart
  // 2. finalJoinJob -- we'll pass the covered range in so that the finalJoin knows how to stitch bootstraps with computed values
  // (stitching only occurs over partitions, no intra-partition stitching)
  def getCoveringRanges(join: api.Join, fullRange: PartitionRange): Seq[BootstrapOrchestrationResult] = {
    /*
      0. Modify compile to put BootstrapPart directly onto a joinPart if it's schema-covering
      1. For each joinPart that has a bootstrapPart
      1. a. Get the intersecting range of the bootstrap part table and `fullRange` -- this is existingBootstrapRange
      1. b. Get the computable range within the `fullRange` based on source table partitions -- this is computableRange
      1. c. create a BootstrapResult with the above two ranges
      1. d. for joinParts without an existing or computable range, do nothing
     */
    Seq.empty
  }
}

// computableRange -- The output for this bootstrapPart does not yet exist, but it can be computed given existing upstream data
// existingBootstrapRange -- The output range for this bootstrapPart already exists
case class BootstrapOrchestrationResult(joinPart: api.JoinPart,
                                        computableRange: Option[PartitionRange],
                                        existingBootstrapRange: Option[PartitionRange])
