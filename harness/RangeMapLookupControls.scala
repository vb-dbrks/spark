/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.mutable.LinkedHashSet
import scala.concurrent.duration._
import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{GetArrayItem, GetMapValue, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.classic.ExpressionColumnNode
import org.apache.spark.sql.execution.RangeExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/** Standalone follow-up harness; compiled outside the Spark source tree. */
object RangeMapLookupControls extends SqlBasedBenchmark {
  private val numRows = 1000000
  private var evidenceDir: Path = _
  @volatile private var sink = 0L

  private case class Scenario(
      dataType: DataType,
      distribution: String,
      size: Int,
      hit: Boolean = true) {
    def id: String = s"${dataType.typeName}-$distribution-$size" + (if (hit) "" else "-miss")
  }

  private val scenarios = Seq(
    Scenario(FloatType, "dense", 1000),
    Scenario(DoubleType, "dense", 1000),
    Scenario(FloatType, "random", 10000),
    Scenario(DoubleType, "random", 10000),
    Scenario(FloatType, "sparse", 10000),
    Scenario(DoubleType, "sparse", 10000),
    Scenario(IntegerType, "dense", 10000),
    Scenario(IntegerType, "random", 10000),
    Scenario(FloatType, "random", 10000, hit = false),
    Scenario(DoubleType, "random", 10000, hit = false),
    Scenario(IntegerType, "random", 10000, hit = false))

  private def save(name: String, text: String): Unit = {
    Files.write(evidenceDir.resolve(name), text.getBytes(UTF_8), StandardOpenOption.CREATE_NEW)
  }

  private def convert(value: Double, dataType: DataType): Any = dataType match {
    case FloatType => value.toFloat
    case DoubleType => value
    case IntegerType => value.toInt
  }

  private def keys(s: Scenario): Array[Any] = {
    val random = new Random(52878)
    val unique = LinkedHashSet.empty[Any]
    while (unique.size < s.size) {
      val value = s.distribution match {
        case "dense" => unique.size.toDouble
        case "sparse" => unique.size * 17.0 + 1.0
        case "random" if s.dataType == IntegerType => random.nextInt(Int.MaxValue).toDouble
        case "random" => random.nextDouble() * 1000000.0
      }
      unique += convert(value, s.dataType)
    }
    require(unique.size == s.size)
    val result = unique.toArray
    if (s.distribution == "random" && s.dataType != IntegerType) {
      require(result.exists { value =>
        val number = value.asInstanceOf[Number].doubleValue()
        number != math.floor(number)
      }, "Random floating keys must include fractional values")
    }
    result
  }

  private def capture(s: Scenario, query: DataFrame, candidate: Boolean): Unit = {
    val qe = query.queryExecution
    require(qe.executedPlan.collect { case _: InMemoryTableScanExec => true }.nonEmpty,
      s"Timed query must read the materialized cache: ${s.id}")
    val expectedMapType = MapType(s.dataType, LongType, valueContainsNull = false)
    def checkLookups(lookups: Seq[GetMapValue]): String = {
      require(lookups.nonEmpty, s"Lookup optimized away for ${s.id}")
      lookups.map { lookup =>
        require(lookup.child.dataType == expectedMapType)
        require(lookup.child.foldable)
        require(lookup.key.dataType == s.dataType && !lookup.key.foldable)
        s"map=${lookup.child.dataType},mapFoldable=${lookup.child.foldable}," +
          s"key=${lookup.key.dataType},keyFoldable=${lookup.key.foldable}"
      }.mkString("\n")
    }
    val analyzed = checkLookups(qe.analyzed.flatMap(_.expressions.flatMap(_.collect {
      case lookup: GetMapValue => lookup
    })))
    val optimized = checkLookups(qe.optimizedPlan.flatMap(_.expressions.flatMap(_.collect {
      case lookup: GetMapValue => lookup
    })))
    val code = qe.debug.codegenToSeq().map(_._2).mkString("\n")
    require(code.contains("mapLookupBuckets"), s"Expected generated hash lookup: ${s.id}")
    val expectMixer = candidate && s.dataType != IntegerType
    require(code.contains("Murmur3_x86_32") == expectMixer,
      s"Unexpected generated hash implementation: ${s.id}")
    save(s"${s.id}-types.txt", s"ANALYZED\n$analyzed\nOPTIMIZED\n$optimized\n")
    save(s"${s.id}-plan.txt", qe.toString)
    save(s"${s.id}-code.java", code)
  }

  private def runCase(s: Scenario, candidate: Boolean): Unit = {
    val ks = keys(s)
    val map = Literal.create(
      new ArrayBasedMapData(new GenericArrayData(ks),
        new GenericArrayData(ks.indices.map(_.toLong).toArray)),
      MapType(s.dataType, LongType, valueContainsNull = false))
    // Match the main matrix: random positive map keys and absent negative integral lookups.
    val lookupKeys = if (s.hit) ks else {
      val missing = ks.indices.map(i => convert(-1.0 - i, s.dataType)).toArray
      val present = ks.toSet
      require(missing.distinct.length == s.size)
      require(missing.forall(key => !present.contains(key)),
        "Missing keys must remain absent after conversion to the actual lookup type")
      missing
    }
    val array = Literal.create(new GenericArrayData(lookupKeys),
      ArrayType(s.dataType, containsNull = false))
    val arrayColumn = new Column(ExpressionColumnNode(array))
    val source = spark.range(0L, numRows.toLong, 1L, 1)
      .select(arrayColumn.getItem((col("id") % lit(s.size)).cast(IntegerType)).as("key"))
    val sourceQe = source.queryExecution
    require(sourceQe.executedPlan.collect { case _: RangeExec => true }.nonEmpty,
      "Input must originate from Spark Range")
    val arrayLookups = sourceQe.analyzed.flatMap(_.expressions.flatMap(_.collect {
      case lookup: GetArrayItem => lookup
    }))
    require(arrayLookups.nonEmpty)
    require(arrayLookups.forall { lookup =>
      lookup.child.foldable && lookup.child.dataType == ArrayType(s.dataType, false) &&
        lookup.ordinal.dataType == IntegerType && !lookup.ordinal.foldable
    })
    val sourcePlan = sourceQe.toString
    require(!Seq("ExistingRDD", "LogicalRDD", "ParallelCollection").exists(sourcePlan.contains))
    save(s"${s.id}-input-plan.txt", sourcePlan)
    val input = source.cache()
    require(input.count() == numRows)
    require(numRows % s.size == 0)
    val expected = if (s.hit) {
      (numRows.toLong / s.size) * s.size * (s.size - 1L) / 2L
    } else {
      -numRows.toLong
    }
    try {
      val column = new Column(ExpressionColumnNode(map))
      val query = input.select(column.getItem(col("key")).as("value"))
        .agg(sum(coalesce(col("value"), lit(-1L))).as("checksum"))
      def consume(): Unit = {
        val actual = query.collect().head.getLong(0)
        require(actual == expected, s"${s.id}: $actual != $expected")
        sink = actual
      }
      consume()
      capture(s, query, candidate)
      save(s"${s.id}-checksum.txt", s"rows=$numRows,entries=${s.size},expected=$expected\n")
      save(s"${s.id}-keys.txt", ks.mkString("\n"))
      save(s"${s.id}-lookup-keys.txt", lookupKeys.mkString("\n"))
      val benchmark = new Benchmark(s.id, numRows, minNumIters = 5,
        warmupTime = 5.seconds, minTime = Duration.Zero, outputPerIteration = true)
      benchmark.addCase(s.id, numIters = 5) { iteration =>
        val start = System.nanoTime()
        consume()
        val elapsed = System.nanoTime() - start
        if (iteration >= 0) {
          val line = s"${s.id},$iteration,$elapsed\n"
          Files.write(evidenceDir.resolve("samples.csv"), line.getBytes(UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        }
      }
      benchmark.run()
    } finally {
      input.unpersist(blocking = true)
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    require(mainArgs.length >= 2,
      "Supply output directory, baseline|candidate, and optional reverse")
    evidenceDir = Paths.get(mainArgs(0))
    Files.createDirectories(evidenceDir)
    require(!Files.exists(evidenceDir.resolve("samples.csv")),
      "Refusing to append to prior results")
    require(Set("baseline", "candidate").contains(mainArgs(1)))
    val candidate = mainArgs(1) == "candidate"
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
    spark.conf.set(SQLConf.MAX_TO_STRING_FIELDS.key, "50")
    require(spark.sparkContext.master == "local[1]")
    require(SQLConf.get.getConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD) == 1000)
    require(SQLConf.get.getConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED))
    require(SQLConf.get.getConf(SQLConf.CODEGEN_FACTORY_MODE).toString == "FALLBACK")
    val origin = classOf[GetMapValue].getProtectionDomain.getCodeSource.getLocation.toURI
    require(origin == Paths.get(sys.props("map.lookup.expected.catalyst")).toUri)
    save("loaded-catalyst.txt", origin.toString)
    val runtime = ManagementFactory.getRuntimeMXBean
    save("environment.txt", s"${runtime.getVmName} ${runtime.getVmVersion}\n" +
      s"${runtime.getInputArguments}\nmaxHeap=${Runtime.getRuntime.maxMemory()}\n" +
      s"rows=$numRows,seed=52878,warmupSeconds=5,measuredIterations=5\n" +
      s"${spark.conf.getAll.toSeq.sorted.mkString("\n")}\n")
    val ordered = if (mainArgs.contains("reverse")) scenarios.reverse else scenarios
    save("case-order.txt", ordered.map(_.id).mkString("\n"))
    ordered.foreach(runCase(_, candidate))
  }
}
