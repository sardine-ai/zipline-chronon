package ai.chronon.orchestration.temporal

import ai.chronon.api
import ai.chronon.orchestration.NodeRunStatus
import slick.ast.BaseTypedType
import slick.jdbc.JdbcType
import slick.jdbc.PostgresProfile.api._

/** Domain model types for the orchestration system.
  *
  * This file contains the core domain model classes used throughout the orchestration system,
  * particularly with Temporal workflows and persistence. These types are used to represent
  * the entities in our system in a type-safe manner.
  *
  * Each type has a clear meaning and purpose:
  * - `TableName`: Represents a data table in the system
  * - `NodeName`: Identifies a processing node in the computation graph
  * - `Branch`: Identifies a branch (similar to git branches) for versioning
  * - `StepDays`: Defines the time window size for processing in days
  * - `NodeExecutionRequest`: Contains all the parameters needed to execute a node
  * - `NodeRunStatus`: Represents the execution status of a node run
  */

/** Represents a table name in the data storage system */
case class TableName(name: String)

/** Identifies a processing node by name */
case class NodeName(name: String)

/** Identifies a branch for versioning of nodes */
case class Branch(branch: String)

/** Specifies the window size in days for processing a node */
case class StepDays(stepDays: Int)

/** Request model for executing a node with specific parameters */
case class NodeExecutionRequest(nodeName: NodeName, branch: Branch, partitionRange: api.PartitionRange)

/** Type mappers for Slick database integration.
  *
  * This object provides bidirectional mappings between our domain model types and database types
  * for use with the Slick ORM. These mappers enable seamless conversion between Scala case classes
  * and database column values.
  *
  * Each mapper defines:
  * 1. How to convert from our domain type to the database type
  * 2. How to convert from the database type back to our domain type
  */
object CustomSlickColumnTypes {

  /** Converts NodeName to/from String columns */
  implicit val nodeNameColumnType: JdbcType[NodeName] with BaseTypedType[NodeName] =
    MappedColumnType.base[NodeName, String](
      _.name, // map NodeName to String
      NodeName // map String to NodeName
    )

  /** Converts Branch to/from String columns */
  implicit val branchColumnType: JdbcType[Branch] with BaseTypedType[Branch] =
    MappedColumnType.base[Branch, String](
      _.branch, // map Branch to String
      Branch // map String to Branch
    )

  /** Converts NodeRunStatus to/from String columns */
  implicit val nodeRunStatusColumnType: JdbcType[NodeRunStatus] with BaseTypedType[NodeRunStatus] =
    MappedColumnType.base[NodeRunStatus, String](
      _.name(), // map NodeRunStatus to String
      name => NodeRunStatus.valueOf(name) // map String to NodeRunStatus
    )

  /** Converts StepDays to/from Int columns */
  implicit val stepDaysColumnType: JdbcType[StepDays] with BaseTypedType[StepDays] =
    MappedColumnType.base[StepDays, Int](
      _.stepDays, // map StepDays to Int
      StepDays // map Int to StepDays
    )
}
