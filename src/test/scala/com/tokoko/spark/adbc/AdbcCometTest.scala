package com.tokoko.spark.adbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager
import scala.util.{Try, Using}

class AdbcCometTest extends AnyFunSuite with BeforeAndAfterAll {

  private val pgHost = sys.env.getOrElse("BENCH_PG_HOST", "localhost")
  private val pgPort = sys.env.getOrElse("BENCH_PG_PORT", "5432")
  private val pgDb = "bench"
  private val pgUser = "bench"
  private val pgPass = "bench"

  private val adbcUri = s"postgresql://$pgUser:$pgPass@$pgHost:$pgPort/$pgDb"
  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"
  private val table = "benchmark_data"

  private var spark: SparkSession = _
  private var pgAvailable: Boolean = false

  override def beforeAll(): Unit = {
    pgAvailable = Try {
      Using.resource(DriverManager.getConnection(
        s"jdbc:postgresql://$pgHost:$pgPort/$pgDb", pgUser, pgPass
      )) { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          stmt.executeQuery(s"SELECT 1 FROM $table LIMIT 1").next()
        }
      }
    }.isSuccess

    if (pgAvailable) {
      spark = SparkSession.builder()
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config("spark.comet.exec.all.enabled", "true")
        .config("spark.comet.sparkToColumnar.enabled", "true")
        .config("spark.comet.sparkToColumnar.supportedOperatorList",
          "Range,InMemoryTableScan,RDDScan,BatchScan")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def readAdbc: org.apache.spark.sql.DataFrame = {
    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "postgresql")
      .option("uri", adbcUri)
      .option("dbtable", table)
      .load()
  }

  test("withColumn should avoid ColumnarToRow with Comet") {
    if (!pgAvailable) cancel("PostgreSQL not available. Start with: docker compose up -d")

    val df = readAdbc
      .withColumn("doubled_value", col("value") * 2)
      .limit(10)

    println("\n=== Physical Plan ===")
    df.explain(true)

    val plan = df.queryExecution.executedPlan
    val planStr = plan.toString()
    val hasSparkColumnarToRow = planStr.contains("ColumnarToRow") &&
      !planStr.replace("CometColumnarToRow", "").replace("CometSparkColumnarToColumnar", "")
        .contains("ColumnarToRow")
    println(s"\n=== Spark ColumnarToRow present (bad): ${!hasSparkColumnarToRow && planStr.contains("ColumnarToRow")} ===")
    println(s"=== Comet native operators active: ${planStr.contains("CometProject")} ===")

    val results = df.collect()
    println(s"\n=== Collected ${results.length} rows ===")
    results.foreach(println)
  }
}
