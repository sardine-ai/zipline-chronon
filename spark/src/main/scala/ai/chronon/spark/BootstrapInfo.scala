/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Constants, ExternalPart, JoinPart, PartitionRange, PartitionSpec, StructField}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructType}
import org.slf4j.{Logger, LoggerFactory}
import ai.chronon.spark.catalog.TableUtils

import scala.collection.{Seq, immutable, mutable}
import scala.util.Try

case class JoinPartMetadata(
    joinPart: JoinPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField],
    derivationDependencies: Map[StructField, Seq[StructField]]
)

case class ExternalPartMetadata(
    externalPart: ExternalPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField]
)

case class BootstrapInfo(
    joinConf: api.Join,
    // join parts enriched with expected output schema
    joinParts: Seq[JoinPartMetadata],
    // external parts enriched with expected schema
    externalParts: Seq[ExternalPartMetadata],
    // derivations schema
    derivations: Array[StructField],
    hashToSchema: Map[String, Array[StructField]]
) {

  lazy val fieldNames: Set[String] = fields.map(_.name).toSet

  private lazy val fields: Seq[StructField] = {
    joinParts.flatMap(_.keySchema) ++ joinParts.flatMap(_.valueSchema) ++
      externalParts.flatMap(_.keySchema) ++ externalParts.flatMap(_.valueSchema) ++ derivations
  }
  private lazy val fieldsMap: Map[String, StructField] = fields.map(f => f.name -> f).toMap

  lazy val baseValueNames: Seq[String] = baseValueFields.map(_.name)
  private lazy val baseValueFields: Seq[StructField] = {
    joinParts.flatMap(_.valueSchema) ++ externalParts.flatMap(_.valueSchema)
  }
}

object BootstrapInfo {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Build metadata for the join that contains schema information for join parts, external parts and bootstrap parts
  def from(joinConf: api.Join,
           range: PartitionRange,
           tableUtils: TableUtils,
           leftSchema: Option[StructType],
           externalPartsAlreadyIncluded: Boolean = false): BootstrapInfo = {

    implicit val tu = tableUtils
    implicit val partitionSpec: PartitionSpec = tableUtils.partitionSpec
    // Enrich each join part with the expected output schema
    logger.info(s"\nCreating BootstrapInfo for GroupBys for Join ${joinConf.metaData.name}")
    var joinPartMetadataSeq: Seq[JoinPartMetadata] = Option(joinConf.joinParts.toScala)
      .getOrElse(Seq.empty)
      .map(part => {
        val gb = GroupBy.from(part.groupBy, range, tableUtils, computeDependency = true)
        val keySchema = SparkConversions
          .toChrononSchema(gb.keySchema)
          .map(field => StructField(part.rightToLeft(field._1), field._2))

        val keyAndPartitionFields =
          gb.keySchema.fields ++ Seq(org.apache.spark.sql.types.StructField(tableUtils.partitionColumn, StringType))
        // todo: this change is only valid for offline use case
        // we need to revisit logic for the logging part to make sure the derived columns are also logged
        // to make bootstrap continue to work
        val outputSchema = if (part.groupBy.hasDerivations) {

          val sparkSchema = StructType(
            SparkConversions.fromChrononSchema(gb.outputSchema).fields ++
              keyAndPartitionFields
          )

          val dummyOutputDf = tableUtils.sparkSession
            .createDataFrame(tableUtils.sparkSession.sparkContext.parallelize(immutable.Seq[Row]()), sparkSchema)

          val finalOutputColumns = part.groupBy.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq
          val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)

          val columns = SparkConversions.toChrononSchema(
            StructType(derivedDummyOutputDf.schema.filterNot(keyAndPartitionFields.contains)))

          api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))
        } else {
          gb.outputSchema
        }
        val valueSchema = outputSchema.fields.map(part.constructJoinPartSchema)
        JoinPartMetadata(part, keySchema, valueSchema, Map.empty) // will be populated below
      })

    // Enrich each external part with the expected output schema
    logger.info(s"\nCreating BootstrapInfo for ExternalParts for Join ${joinConf.metaData.name}")
    val externalParts: Seq[ExternalPartMetadata] = if (externalPartsAlreadyIncluded) {
      Seq.empty
    } else {
      Option(joinConf.onlineExternalParts.toScala)
        .getOrElse(Seq.empty)
        .map(part => ExternalPartMetadata(part, part.keySchemaFull, part.valueSchemaFull))
    }

    val leftFields = leftSchema
      .map(schema => SparkConversions.toChrononSchema(schema))
      .map(_.map(field => StructField(field._1, field._2)))
      .getOrElse(Array.empty[StructField])
    val baseFields = joinPartMetadataSeq.flatMap(_.valueSchema) ++ externalParts.flatMap(_.valueSchema) ++ leftFields
    val sparkSchema = StructType(SparkConversions.fromChrononSchema(api.StructType("", baseFields.toArray)))

    val baseDf = tableUtils.sparkSession.createDataFrame(
      tableUtils.sparkSession.sparkContext.parallelize(immutable.Seq[Row]()),
      sparkSchema
    )

    val derivedSchema = if (joinConf.hasDerivations) {
      val projections = joinConf.derivationsScala.derivationProjection(baseDf.columns)
      val projectionMap = projections.toMap
      val derivedDf = baseDf.select(
        projections.map { case (name, expression) =>
          expr(expression).as(name)
        }.toSeq: _*
      )
      SparkConversions.toChrononSchema(derivedDf.schema).map { case (name, dataType) =>
        (StructField(name, dataType), projectionMap(name))
      }
    } else {
      Array.empty[(StructField, String)]
    }

    /*
     * Partition bootstrap into log-based versus regular table-based.
     *
     * Log table requires special handling because unlike regular Hive table, since log table is affected by schema
     * evolution. A NULL column does not simply mean that the correct value was NULL, it could be that for this record,
     * the value was never logged as the column was added more recently. Instead, we must use the schema info encoded
     * in the schema_hash at time of logging to tell whether a column truly was populated or not.
     *
     * For standard Hive tables however, we can simply trust that a NULL column means that its CORRECT value is NULL,
     * that is, we trust all the data that is given to Chronon by its user and it takes precedence over backfill.
     */
    val (logBootstrapParts, tableBootstrapParts) =
      Option(joinConf.bootstrapParts.toScala).getOrElse(Seq.empty).partition { part =>
        // treat log table with additional selects as standard table bootstrap
        val hasSelect = part.isSetQuery && part.query.isSetSelects
        if (!tableUtils.tableReachable(part.table)) {
          throw new Exception(s"Bootstrap table ${part.table} does NOT exist!")
        }
        val tblProps = tableUtils.getTableProperties(part.table)
        tblProps.isDefined && tblProps.get.contains(Constants.ChrononLogTable) && !hasSelect
      }

    val exceptionList = mutable.ListBuffer[Throwable]()
    def collectException(assertion: => Unit): Unit = Try(assertion).failed.foreach(exceptionList += _)

    logger.info(s"\nCreating BootstrapInfo for Log Based Bootstraps for Join ${joinConf.metaData.name}")
    // Verify that join keys are valid columns on the log table
    logBootstrapParts
      .foreach(part => {
        // practically there should only be one logBootstrapPart per Join, but nevertheless we will loop here
        val schema = tableUtils.getSchemaFromTable(part.table)
        val missingKeys = part
          .keys(joinConf, part.query.partitionSpec(tableUtils.partitionSpec).column)
          .filterNot(schema.fieldNames.contains)
        collectException(assert(
          missingKeys.isEmpty,
          s"Log table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}, table schema: ${schema.pretty}"
        ))
      })

    // Retrieve schema_hash mapping info from Hive table properties
    val logHashes = logBootstrapParts.flatMap { part =>
      LogFlattenerJob
        .readSchemaTableProperties(tableUtils, part.table)
        .mapValues(LoggingSchema.parseLoggingSchema(_).valueFields.fields)
        .toSeq
    }.toMap

    logger.info(s"\nCreating BootstrapInfo for Table Based Bootstraps for Join ${joinConf.metaData.name}")
    // Verify that join keys are valid columns on the bootstrap source table
    val tableHashes = tableBootstrapParts
      .map(part => {
        val range = PartitionRange(part.startPartition, part.endPartition)
        val bootstrapDf =
          tableUtils
            .scanDf(part.query,
                    part.table,
                    Some(Map(part.query.partitionSpec(tableUtils.partitionSpec).column -> null)),
                    range = Some(range))
        val schema = bootstrapDf.schema
        // We expect partition column and not effectivePartitionColumn because of the scanDf rename
        val missingKeys = part.keys(joinConf, tableUtils.partitionColumn).filterNot(schema.fieldNames.contains)
        collectException(
          assert(
            missingKeys.isEmpty,
            s"Table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}, schema: ${schema.pretty}"
          ))

        collectException(
          assert(
            !bootstrapDf.columns.contains(Constants.SchemaHash),
            s"${Constants.SchemaHash} is a reserved column that should only be used for chronon log tables"
          ))

        val valueFields = SparkConversions
          .toChrononSchema(schema)
          .filterNot { case (name, _) =>
            part.keys(joinConf, tableUtils.partitionColumn).contains(name) || name == "ts"
          }
          .map(field => StructField(field._1, field._2))

        part.semanticHash -> (valueFields, part.table)
      })
      .toMap

    /*
     * hashToSchema is a mapping from a hash string to an associated list of structField, such that if a record was
     * marked by this hash, it means that all the associated structFields have been pre-populated.

     * logic behind the hash:
     * - for chronon flattened log table: we use the schema_hash recorded at logging time generated per-row
     * - for regular hive tables: we use the semantic_hash (a snapshot) of the bootstrap part object
     */
    val hashToSchema = logHashes ++ tableHashes.mapValues(_._1)

    /*
     * For each join part, find a mapping from a derived field to the subset of base fields that this derived field
     * depends on. Skip the ones that don't have any dependency on the join part.
     */
    val identifierRegex = "[a-zA-Z_][a-zA-Z0-9_]*".r
    def findDerivationDependencies(joinPartMetadata: JoinPartMetadata): Map[StructField, Seq[StructField]] = {
      if (derivedSchema.isEmpty) {
        joinPartMetadata.valueSchema.map { structField => structField -> Seq(structField) }.toMap
      } else {
        derivedSchema.flatMap { case (derivedField, expression) =>
          // Check if the expression contains any fields from the join part by string matching.
          val identifiers = identifierRegex.findAllIn(expression).toSet
          val requiredBaseColumns = joinPartMetadata.valueSchema.filter(f => identifiers(f.name))
          if (requiredBaseColumns.nonEmpty) {
            Some(derivedField -> requiredBaseColumns.toSeq)
          } else {
            None
          }
        }.toMap
      }
    }

    joinPartMetadataSeq = joinPartMetadataSeq.map { part =>
      part.copy(derivationDependencies = findDerivationDependencies(part))
    }
    val bootstrapInfo =
      BootstrapInfo(joinConf, joinPartMetadataSeq, externalParts, derivedSchema.map(_._1), hashToSchema)

    // validate that all selected fields except keys from (non-log) bootstrap tables match with
    // one of defined fields in join parts or external parts
    for (
      (fields, table) <- tableHashes.values;
      field <- fields
    ) yield {

      collectException(
        assert(
          bootstrapInfo.fieldsMap.contains(field.name),
          s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} does NOT have this field
           |""".stripMargin
        ))
      collectException(
        assert(
          bootstrapInfo.fieldsMap(field.name) == field,
          s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} has the same field with ${bootstrapInfo
            .fieldsMap(field.name)
            .fieldType}
           |""".stripMargin
        ))
    }
    def stringify(schema: Array[StructField]): String = {
      if (schema.isEmpty) {
        "<empty>"
      } else {
        SparkConversions.fromChrononSchema(api.StructType("", schema)).pretty
      }
    }

    if (exceptionList.nonEmpty) {
      exceptionList.foreach(t => logger.error(t.traceString))
      throw new Exception(s"Validation failed for bootstrapInfo construction for join ${joinConf.metaData.name}")
    }

    logger.info(s"\n======= Finalized Bootstrap Info ${joinConf.metaData.name} =======\n")
    joinPartMetadataSeq.foreach { metadata =>
      logger.info(s"""Bootstrap Info for Join Part `${metadata.joinPart.groupBy.metaData.name}`
           |Key Schema:
           |${stringify(metadata.keySchema)}
           |Value Schema:
           |${stringify(metadata.valueSchema)}
           |""".stripMargin)
    }
    externalParts.foreach { metadata =>
      logger.info(s"""Bootstrap Info for External Part `${metadata.externalPart.fullName}`
           |Key Schema:
           |${stringify(metadata.keySchema)}
           |Value Schema:
           |${stringify(metadata.valueSchema)}
           |""".stripMargin)
    }
    if (derivedSchema.nonEmpty) {
      logger.info(s"""Bootstrap Info for Derivations
                 |${stringify(derivedSchema.map(_._1))}
                 |""".stripMargin)
    }
    logger.info(s"""Bootstrap Info for Log Bootstraps
         |Log Hashes: ${logHashes.keys.prettyInline}
         |""".stripMargin)
    tableHashes.foreach { case (hash, (schema, _)) =>
      logger.info(s"""Bootstrap Info for Table Bootstraps
           |Table Hash: $hash
           |Bootstrap Schema:
           |${stringify(schema)}
           |""".stripMargin)
    }

    logger.info(s"\n======= Finalized Bootstrap Info ${joinConf.metaData.name} END =======\n")

    bootstrapInfo
  }
}
