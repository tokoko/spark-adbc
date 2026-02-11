package com.tokoko.spark.adbc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.DriverManager
import scala.util.{Try, Using}

class AdbcJdbcBenchmarkTest extends AnyFunSuite with BeforeAndAfterAll {

  private val pgHost = sys.env.getOrElse("BENCH_PG_HOST", "localhost")
  private val pgPort = sys.env.getOrElse("BENCH_PG_PORT", "5432")
  private val pgDb = "bench"
  private val pgUser = "bench"
  private val pgPass = "bench"

  private val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"
  private val adbcUri = s"postgresql://$pgUser:$pgPass@$pgHost:$pgPort/$pgDb"
  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"

  private val warmupRuns = 0
  private val timedRuns = 1
  private val table = "benchmark_data"

  private var spark: SparkSession = _
  private var pgAvailable: Boolean = false
  private var rowCount: Long = 0

  override def beforeAll(): Unit = {
    pgAvailable = Try {
      Using.resource(DriverManager.getConnection(jdbcUrl, pgUser, pgPass)) { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          val rs = stmt.executeQuery(s"SELECT count(*) FROM $table")
          rs.next()
          rowCount = rs.getLong(1)
          println(s"PostgreSQL available. $table has $rowCount rows.")
        }
      }
    }.isSuccess

    if (pgAvailable) {
      spark = SparkSession.builder()
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    }
  }

  private val tempDir = new File(System.getProperty("java.io.tmpdir"), "adbc-bench")

  private def consume(df: DataFrame): Unit = {
    df.write.mode("overwrite").parquet(tempDir.getAbsolutePath)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteRecursive(tempDir)
  }

  private def deleteRecursive(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursive)
    f.delete()
  }

  private def readAdbc(dbtable: String): DataFrame = {
    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "postgresql")
      .option("uri", adbcUri)
      .option("dbtable", dbtable)
      .load()
  }

  private def readJdbc(dbtable: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("user", pgUser)
      .option("password", pgPass)
      .load()
  }

  private def benchmark(label: String)(block: => Unit): BenchResult = {
    // warmup
    for (_ <- 1 to warmupRuns) block

    // timed runs
    val times = (1 to timedRuns).map { _ =>
      val start = System.nanoTime()
      block
      (System.nanoTime() - start) / 1e6
    }

    val result = BenchResult(label, times)
    println(f"  $label%-8s  avg=${result.avg}%8.1f ms  min=${result.min}%8.1f ms  max=${result.max}%8.1f ms  [${times.map(t => f"$t%.0f").mkString(", ")}]")
    result
  }

  private def compareBenchmark(testName: String)(adbcBlock: => Unit)(jdbcBlock: => Unit): Unit = {
    println(s"\n=== $testName ===")
    val adbc = benchmark("ADBC")(adbcBlock)
    val jdbc = benchmark("JDBC")(jdbcBlock)
    val speedup = jdbc.avg / adbc.avg
    println(f"  Speedup: ${speedup}%.2fx (ADBC vs JDBC)")
  }

  private def skipIfNoPg(): Unit = {
    if (!pgAvailable) cancel("PostgreSQL not available. Start with: docker compose up -d")
  }

  test("full table scan") {
    skipIfNoPg()
    compareBenchmark(s"Full Table Scan ($rowCount rows)") {
      consume(readAdbc(table))
    } {
      consume(readJdbc(table))
    }
  }

  test("full table scan - columnar (no row conversion)") {
    skipIfNoPg()
    println(s"\n=== Full Table Scan Columnar ($rowCount rows) ===")
    val adbcResult = benchmark("ADBC") {
      val df = readAdbc(table)
      val plan = df.queryExecution.executedPlan
      val columnarPlan = plan.find(_.supportsColumnar).get
      val rdd = columnarPlan.executeColumnar()
      rdd.foreach(batch => batch.numRows())
    }
    val jdbcResult = benchmark("JDBC") {
      consume(readJdbc(table))
    }
    val speedup = jdbcResult.avg / adbcResult.avg
    println(f"  Speedup: ${speedup}%.2fx (ADBC columnar vs JDBC)")
  }

  test("column projection") {
    skipIfNoPg()
    compareBenchmark("Column Projection (2 of 6 columns)") {
      consume(readAdbc(table).select("id", "value"))
    } {
      consume(readJdbc(table).select("id", "value"))
    }
  }

  test("filtered read") {
    skipIfNoPg()
    compareBenchmark("Filtered Read (category = 'cat_5')") {
      consume(readAdbc(table).filter("category = 'cat_5'"))
    } {
      consume(readJdbc(table).filter("category = 'cat_5'"))
    }
  }

  test("aggregation") {
    skipIfNoPg()
    compareBenchmark("Aggregation (GROUP BY category)") {
      readAdbc(table).groupBy("category").agg(
        Map("value" -> "avg", "amount" -> "sum", "id" -> "count")
      ).collect()
    } {
      readJdbc(table).groupBy("category").agg(
        Map("value" -> "avg", "amount" -> "sum", "id" -> "count")
      ).collect()
    }
  }

  test("full scan with join + filter + collect - comet") {
    skipIfNoPg()

    // Must stop existing session so Comet configs take effect
    spark.stop()

    spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
      .config("spark.comet.enabled", "true")
      .config("spark.comet.exec.enabled", "true")
      .config("spark.comet.exec.all.enabled", "true")
      .config("spark.comet.columnar.shuffle.enabled", "false")
      .getOrCreate()

    def pipeline(reader: String => DataFrame): Array[org.apache.spark.sql.Row] = {
      val a = reader(table).as("a")
      val b = reader(table)
        .groupBy("category")
        .agg(avg("value").as("avg_value"), count("*").as("cnt"))
        .as("b")
      a.join(b, col("a.category") === col("b.category"))
        .filter(col("a.amount") > 900)
        .groupBy(col("b.category"))
        .agg(sum("a.value").as("total"), max("b.avg_value").as("max_avg"), sum("b.cnt").as("total_cnt"))
        .collect()
    }

    // Print plan to verify Comet operators
    val df = readAdbc(table).as("a")
    val agg = readAdbc(table).groupBy("category").agg(avg("value").as("avg_value"), count("*").as("cnt")).as("b")
    val plan = df.join(agg, col("a.category") === col("b.category"))
      .filter(col("a.amount") > 900)
      .groupBy(col("b.category"))
      .agg(sum("a.value"), max("b.avg_value"), sum("b.cnt"))
    println("\n=== Comet Physical Plan ===")
    plan.explain()

    compareBenchmark(s"Join + Filter + Collect Comet ($rowCount rows)") {
      pipeline(readAdbc)
    } {
      pipeline(readJdbc)
    }
  }

  case class BenchResult(label: String, times: Seq[Double]) {
    val avg: Double = times.sum / times.length
    val min: Double = times.min
    val max: Double = times.max
  }
}
