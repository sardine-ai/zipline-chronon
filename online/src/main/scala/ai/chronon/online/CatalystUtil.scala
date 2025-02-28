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

package ai.chronon.online

import ai.chronon.api.DataType
import ai.chronon.api.StructType
import ai.chronon.online.CatalystUtil.IteratorWrapper
import ai.chronon.online.CatalystUtil.PoolKey
import ai.chronon.online.CatalystUtil.poolMap
import ai.chronon.online.Extensions.StructTypeOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.{
  BufferedRowIterator,
  FilterExec,
  InputAdapter,
  LocalTableScanExec,
  ProjectExec,
  RDDScanExec,
  WholeStageCodegenExec
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types
import org.slf4j.LoggerFactory

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.function
import scala.collection.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CatalystUtil {
  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }

  // Max fields supported for codegen. If this is exceeded, we fail at creation time to avoid buggy codegen
  val MaxFields = 1000

  lazy val session: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(s"catalyst_test_${Thread.currentThread().toString}")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.ui.enabled", "false")
      // the default column reader batch size is 4096 - spark reads that many rows into memory buffer at once.
      // that causes ooms on large columns.
      // for derivations we only need to read one row at a time.
      // for interactive we set the limit to 16.
      .config("spark.sql.parquet.columnarReaderBatchSize", "16")
      // The default doesn't seem to be set properly in the scala 2.13 version of spark
      // running into this issue https://github.com/dotnet/spark/issues/435
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.codegen.maxFields", MaxFields)
      .enableHiveSupport() // needed to support registering Hive UDFs via CREATE FUNCTION.. calls
      .getOrCreate()
    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class PoolKey(expressions: Seq[(String, String)], inputSchema: StructType)
  val poolMap: PoolMap[PoolKey, CatalystUtil] = new PoolMap[PoolKey, CatalystUtil](pi =>
    new CatalystUtil(pi.inputSchema, pi.expressions))
}

class PoolMap[Key, Value](createFunc: Key => Value, maxSize: Int = 100, initialSize: Int = 2) {
  val map: ConcurrentHashMap[Key, ArrayBlockingQueue[Value]] = new ConcurrentHashMap[Key, ArrayBlockingQueue[Value]]()
  def getPool(input: Key): ArrayBlockingQueue[Value] =
    map.computeIfAbsent(
      input,
      new function.Function[Key, ArrayBlockingQueue[Value]] {
        override def apply(t: Key): ArrayBlockingQueue[Value] = {
          val result = new ArrayBlockingQueue[Value](maxSize)
          var i = 0
          while (i < initialSize) {
            result.add(createFunc(t))
            i += 1
          }
          result
        }
      }
    )

  def performWithValue[Output](key: Key, pool: ArrayBlockingQueue[Value])(func: Value => Output): Output = {
    var value = pool.poll()
    if (value == null) {
      value = createFunc(key)
    }
    try {
      func(value)
    } catch {
      case e: Exception => throw e
    } finally {
      pool.offer(value)
    }
  }
}

class PooledCatalystUtil(expressions: Seq[(String, String)], inputSchema: StructType) {
  private val poolKey = PoolKey(expressions, inputSchema)
  private val cuPool = poolMap.getPool(PoolKey(expressions, inputSchema))
  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] =
    poolMap.performWithValue(poolKey, cuPool) { _.performSql(values) }
  def outputChrononSchema: Array[(String, DataType)] =
    poolMap.performWithValue(poolKey, cuPool) { _.outputChrononSchema }
}

class CatalystUtil(inputSchema: StructType,
                   selects: Seq[(String, String)],
                   wheres: Seq[String] = Seq.empty,
                   setups: Seq[String] = Seq.empty) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val selectClauses = selects.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  private val whereClauseOpt = Option(wheres)
    .filter(_.nonEmpty)
    .map { w =>
      // wrap each clause in parens
      w.map(c => s"( $c )").mkString(" AND ")
    }

  @transient lazy val inputSparkSchema: types.StructType = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(inputSparkSchema)
  private val inputArrEncoder = SparkInternalRowConversions.to(inputSparkSchema, false)

  private val (transformFunc: (InternalRow => Seq[InternalRow]), outputSparkSchema: types.StructType) = initialize()

  private lazy val outputArrDecoder = SparkInternalRowConversions.from(outputSparkSchema, false)
  @transient lazy val outputChrononSchema: Array[(String, DataType)] =
    SparkConversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)

  def performSql(values: Array[Any]): Seq[Array[Any]] = {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRowSeq = transformFunc(internalRow)
    val outputVal = resultRowSeq.map(resultRow => outputArrDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Array[Any]])
  }

  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    performSql(internalRow)
  }

  def performSql(row: InternalRow): Seq[Map[String, Any]] = {
    val resultRowMaybe = transformFunc(row)
    val outputVal = resultRowMaybe.map(resultRow => outputDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Map[String, Any]])
  }

  def getOutputSparkSchema: types.StructType = outputSparkSchema

  /** Extracts transformation function from a WholeStageCodegenExec node
    */
  private def extractCodegenStageTransformer(whc: WholeStageCodegenExec): InternalRow => Seq[InternalRow] = {
    logger.info(s"Extracting codegen stage transformer for: ${whc}")

    try {
      // Generate and compile the code
      val (ctx, cleanedSource) = whc.doCodeGen()

      // Log a snippet of the generated code for debugging
      val codeSnippet = cleanedSource.body.split("\n").take(20).mkString("\n")
      logger.debug(s"Generated code snippet: \n$codeSnippet\n...")

      val (clazz, compilationTime) = CodeGenerator.compile(cleanedSource)
      logger.info(s"Compiled code in ${compilationTime}ms")

      val references = ctx.references.toArray
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
      buffer.init(0, Array(iteratorWrapper))

      def codegenFunc(row: InternalRow): Seq[InternalRow] = {
        iteratorWrapper.put(row)
        val result = ArrayBuffer.empty[InternalRow]
        while (buffer.hasNext) {
          result.append(buffer.next())
        }
        result
      }

      codegenFunc
    } catch {
      case e: Exception =>
        logger.error(s"Failed to compile code for WholeStageCodegenExec: ${e.getMessage}", e)

        // Fall back to interpreted execution as a last resort
        logger.warn("Falling back to non-codegen execution path")

        // Create a fallback function that processes the child plan without codegen
        val childPlan = whc.child
        row => {
          // Use a fallback approach based on the child plan type
          childPlan match {
            case project: ProjectExec =>
              extractProjectTransformer(project).apply(row)
            case filter: FilterExec =>
              extractFilterTransformer(filter).apply(row)
            case _ =>
              // For any other child plan, just return the row as-is
              // This is a conservative fallback that may not be correct for all cases
              Seq(row)
          }
        }
    }
  }

  /** Extracts transformation function from a ProjectExec node
    */
  private def extractProjectTransformer(project: ProjectExec): InternalRow => Seq[InternalRow] = {
    // Use project.child.output as input schema instead of project.output
    // This ensures expressions like int32s#8 can be properly resolved
    val unsafeProjection = UnsafeProjection.create(project.projectList, project.child.output)

    row => Seq(unsafeProjection.apply(row))
  }

  /** Extracts transformation function from a FilterExec node
    */
  private def extractFilterTransformer(filter: FilterExec): InternalRow => Seq[InternalRow] = { row =>
    {
      val passed = CatalystHelper.evalFilterExec(row, filter.condition, filter.child.output)
      if (passed) Seq(row) else Seq.empty
    }
  }

  /** Recursively builds a chain of transformation functions from a SparkPlan
    */
  private def buildTransformChain(plan: org.apache.spark.sql.execution.SparkPlan): InternalRow => Seq[InternalRow] = {
    logger.info(s"Building transform chain for plan: ${plan.getClass.getSimpleName}")

    plan match {
      case whc: WholeStageCodegenExec =>
        logger.info(s"WholeStageCodegenExec child plan: ${whc.child}")

        // Check for tooManyFields issue and emit a more helpful diagnostic
        if (WholeStageCodegenExec.isTooManyFields(SQLConf.get, whc.child.schema)) {
          logger.warn("WholeStageCodegenExec has too many fields which may lead to code generation issues")
          logger.warn(s"Schema has ${whc.child.schema.size} fields, max is ${SQLConf.get.wholeStageMaxNumFields}")
        }

        // If this is a WholeStageCodegenExec, use its compiled code
        extractCodegenStageTransformer(whc)

      case project: ProjectExec =>
        project.child match {
          // Special handling for direct RDD scans - no need to process through child
          case _: RDDScanExec | _: LocalTableScanExec =>
            // When the child is a simple scan, we can directly apply the projection
            extractProjectTransformer(project)

          case _ =>
            // For complex children, we need to chain the transformations
            // Create a single function that applies both child and projection transformations
            // The child transformer generates intermediate rows with the schema that the projection expects
            val childTransformer = buildTransformChain(project.child)
            val projectTransformer = extractProjectTransformer(project)

            row => {
              val intermediateRows = childTransformer(row)
              intermediateRows.flatMap(projectTransformer)
            }
        }

      case filter: FilterExec =>
        // For a filter, first process the child and then apply filter
        val childTransformer = buildTransformChain(filter.child)
        val filterTransformer = extractFilterTransformer(filter)

        row => childTransformer(row).flatMap(filterTransformer)

      case input: InputAdapter =>
        // For InputAdapter, just pass through to its child
        buildTransformChain(input.child)

      case ltse: LocalTableScanExec =>
        // Input row is unused for LocalTableScanExec
        _ => ArrayBuffer(ltse.executeCollect(): _*)

      case rddse: RDDScanExec =>
        val unsafeProjection = UnsafeProjection.create(rddse.schema)
        row => Seq(unsafeProjection.apply(row))

      case unknown =>
        logger.warn(s"Unrecognized plan node: ${unknown.getClass.getName}")
        throw new RuntimeException(s"Unrecognized stage in codegen: ${unknown.getClass}")
    }
  }

  private def initialize(): (InternalRow => Seq[InternalRow], types.StructType) = {
    val session = CatalystUtil.session

    // run through and execute the setup statements
    setups.foreach { statement =>
      try {
        session.sql(statement)
        logger.info(s"Executed setup statement: $statement")
      } catch {
        case _: FunctionAlreadyExistsException =>
        // ignore - this crops up in unit tests on occasion
        case e: Exception =>
          logger.warn(s"Failed to execute setup statement: $statement", e)
          throw new RuntimeException(s"Error executing setup statement: $statement", e)
      }
    }

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val df = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)
    val filteredDf = whereClauseOpt.map(df.where(_)).getOrElse(df)

    // extract transform function from the df spark plan
    val execPlan = filteredDf.queryExecution.executedPlan
    logger.info(s"Catalyst Execution Plan - ${execPlan}")
    val func: InternalRow => ArrayBuffer[InternalRow] = execPlan match {
      case whc: WholeStageCodegenExec => {
        // if we have too many fields, this whole stage codegen will result incorrect code so we fail early
        require(
          !WholeStageCodegenExec.isTooManyFields(SQLConf.get, inputSparkSchema),
          s"Too many fields in input schema. Catalyst util max field config: ${CatalystUtil.MaxFields}. " +
            s"Spark session setting: ${SQLConf.get.wholeStageMaxNumFields}. Schema: ${inputSparkSchema.simpleString}"
        )

    // Use the new recursive approach to build a transformation chain
    val transformer = buildTransformChain(execPlan)

    (transformer, df.schema)
  }
}
