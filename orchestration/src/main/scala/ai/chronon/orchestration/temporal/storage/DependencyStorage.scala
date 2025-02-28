package ai.chronon.orchestration.temporal.storage

// Storage interface for dependency management
trait DependencyStorage {
  def isDependencyCompleted(nodeId: String): Boolean
  def markDependencyCompleted(nodeId: String): Unit
}
