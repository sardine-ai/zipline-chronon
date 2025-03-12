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
import org.apache.spark.sql.execution.BufferedRowIterator
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.WholeStageCodegenExec
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

// This class by itself it not thread safe because of the transformBuffer
class CatalystUtil(inputSchema: StructType,
                   selects: Seq[(String, String)],
                   wheres: Seq[String] = Seq.empty,
                   setups: Seq[String] = Seq.empty) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  val selectClauses: Seq[String] = selects.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  val whereClauseOpt: Option[String] = Option(wheres)
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
    val func: InternalRow => ArrayBuffer[InternalRow] = filteredDf.queryExecution.executedPlan match {
      case whc: WholeStageCodegenExec => {
        // if we have too many fields, this whole stage codegen will result incorrect code so we fail early
        require(
          !WholeStageCodegenExec.isTooManyFields(SQLConf.get, inputSparkSchema),
          s"Too many fields in input schema. Catalyst util max field config: ${CatalystUtil.MaxFields}. " +
            s"Spark session setting: ${SQLConf.get.wholeStageMaxNumFields}. Schema: ${inputSparkSchema.simpleString}"
        )

        val (ctx, cleanedSource) = whc.doCodeGen()
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val references = ctx.references.toArray
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
        buffer.init(0, Array(iteratorWrapper))
        def codegenFunc(row: InternalRow): ArrayBuffer[InternalRow] = {
          iteratorWrapper.put(row)
          val result = ArrayBuffer.empty[InternalRow]
          while (buffer.hasNext) {
            result.append(buffer.next())
          }
          result
        }
        codegenFunc
      }

      case ProjectExec(projectList, fp @ FilterExec(condition, child)) => {
        val unsafeProjection = UnsafeProjection.create(projectList, fp.output)

        def projectFunc(row: InternalRow): ArrayBuffer[InternalRow] = {
          val r = CatalystHelper.evalFilterExec(row, condition, child.output)
          if (r)
            ArrayBuffer(unsafeProjection.apply(row))
          else
            ArrayBuffer.empty[InternalRow]
        }

        projectFunc
      }
      case ProjectExec(projectList, childPlan) => {
        childPlan match {
          // This WholeStageCodegenExec case is slightly different from the one above as we apply a projection.
          case whc @ WholeStageCodegenExec(_: FilterExec) =>
            val unsafeProjection = UnsafeProjection.create(projectList, childPlan.output)
            val (ctx, cleanedSource) = whc.doCodeGen()
            val (clazz, _) = CodeGenerator.compile(cleanedSource)
            val references = ctx.references.toArray
            val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
            val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
            buffer.init(0, Array(iteratorWrapper))
            def codegenFunc(row: InternalRow): ArrayBuffer[InternalRow] = {
              iteratorWrapper.put(row)
              val result = ArrayBuffer.empty[InternalRow]
              while (buffer.hasNext) {
                result.append(unsafeProjection.apply(buffer.next()))
              }
              result
            }
            codegenFunc
          case _ =>
            val unsafeProjection = UnsafeProjection.create(projectList, childPlan.output)
            def projectFunc(row: InternalRow): ArrayBuffer[InternalRow] = {
              ArrayBuffer(unsafeProjection.apply(row))
            }
            projectFunc
        }
      }
      case ltse: LocalTableScanExec => {
        // Input `row` is unused because for LTSE, no input is needed to compute the output
        def projectFunc(row: InternalRow): ArrayBuffer[InternalRow] =
          ArrayBuffer(ltse.executeCollect(): _*)

        projectFunc
      }
      case rddse: RDDScanExec => {
        val unsafeProjection = UnsafeProjection.create(rddse.schema)
        def projectFunc(row: InternalRow): ArrayBuffer[InternalRow] =
          ArrayBuffer(unsafeProjection.apply(row))

        projectFunc
      }
      case unknown => throw new RuntimeException(s"Unrecognized stage in codegen: ${unknown.getClass}")
    }

    (func, df.schema)
  }
}
