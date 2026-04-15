package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

abstract class AdbcTestBase extends AnyFunSuite with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  protected def setupDatabase(): Unit
  protected def teardownDatabase(): Unit

  /** Returns a DataFrameReader pre-configured with driver-specific options. */
  protected def adbcReader: DataFrameReader

  /** Raw ADBC connect parameters used for the executeSchema probe. */
  protected def adbcDriver: String
  protected def adbcParams: Map[String, Object]

  protected def readTable: DataFrame =
    adbcReader.option("dbtable", "employees").load()

  protected def writeTo(df: DataFrame, dbtable: String): Unit = {
    val base = df.write.format("com.tokoko.spark.adbc")
      .option("driver", adbcDriver)
      .option("dbtable", dbtable)
      .mode("append")
    adbcParams.foldLeft(base) { case (w, (k, v)) => w.option(k, v.toString) }.save()
  }

  protected def readTablePartitioned: DataFrame =
    adbcReader
      .option("dbtable", "employees")
      .option("partitionColumn", "id")
      .option("lowerBound", "1")
      .option("upperBound", "4")
      .option("numPartitions", "3")
      .load()

  override def beforeAll(): Unit = {
    setupDatabase()
    spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    teardownDatabase()
  }

  test("read via query option") {
    val df = adbcReader.option("query", "SELECT * FROM employees").load()
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("id", "name", "salary"))
  }

  test("read via dbtable option") {
    val df = readTable
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("id", "name", "salary"))
  }

  test("column pruning") {
    val df = readTable.select("name", "salary")
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("name", "salary"))
  }

  test("filter pushdown - equality") {
    val df = readTable.filter("name = 'Robin'")
    df.show()
    assert(df.count() == 1)
    assert(df.collect()(0).getAs[String]("name") == "Robin")
  }

  test("filter pushdown - comparison") {
    val df = readTable.filter("salary > 2000")
    df.show()
    assert(df.count() == 2)
  }

  test("filter pushdown with column pruning") {
    val df = readTable.select("name", "salary").filter("salary >= 3000")
    df.show()
    assert(df.count() == 2)
    assert(df.columns.toSet == Set("name", "salary"))
  }

  test("limit pushdown") {
    val df = readTable.limit(2)
    df.show()
    assert(df.count() == 2)
  }

  test("topN pushdown - descending") {
    val df = readTable.orderBy(col("salary").desc).limit(1)
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getAs[String]("name") == "Alice")
  }

  test("topN pushdown - ascending") {
    val df = readTable.orderBy(col("salary").asc).limit(2)
    df.show()
    val rows = df.collect()
    assert(rows.length == 2)
    val names = rows.map(_.getAs[String]("name")).toSet
    assert(names == Set("Tornike", "Robin"))
  }

  test("topN pushdown with filter and column pruning") {
    val df = readTable
      .select("name", "salary")
      .filter("salary > 2000")
      .orderBy(col("salary").desc)
      .limit(1)
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getAs[String]("name") == "Alice")
    assert(df.columns.toSet == Set("name", "salary"))
  }

  test("aggregate pushdown - count") {
    val df = readTable.agg(count("*"))
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getLong(0) == 3)
  }

  test("aggregate pushdown - sum, min, max") {
    val df = readTable.agg(sum("salary"), min("salary"), max("salary"))
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).get(0).asInstanceOf[Number].longValue() == 9000) // sum
    assert(rows(0).get(1).asInstanceOf[Number].longValue() == 2000) // min
    assert(rows(0).get(2).asInstanceOf[Number].longValue() == 4000) // max
  }

  test("aggregate pushdown - group by") {
    val df = readTable.groupBy("name").agg(sum("salary").as("total"))
    df.show()
    assert(df.count() == 3)
  }

  test("aggregate pushdown - group by with filter") {
    val df = readTable.filter("salary > 2000").groupBy("name").agg(sum("salary").as("total"))
    df.show()
    assert(df.count() == 2)
  }

  test("partitioned read - all rows returned") {
    val df = readTablePartitioned
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("id", "name", "salary"))
  }

  test("partitioned read - correct number of partitions") {
    val df = readTablePartitioned
    assert(df.rdd.getNumPartitions == 3)
  }

  test("partitioned read with filter") {
    val df = readTablePartitioned.filter("salary > 2000")
    df.show()
    assert(df.count() == 2)
  }

  test("partitioned read with aggregation") {
    val df = readTablePartitioned.agg(sum("salary"), min("salary"), max("salary"))
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).get(0).asInstanceOf[Number].longValue() == 9000)
    assert(rows(0).get(1).asInstanceOf[Number].longValue() == 2000)
    assert(rows(0).get(2).asInstanceOf[Number].longValue() == 4000)
  }

  test("reserved keyword as column name") {
    val df = adbcReader.option("dbtable", "reserved_kw").load()
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("id", "order"))
  }

  test("reserved keyword with filter") {
    val df = adbcReader.option("dbtable", "reserved_kw").load().filter("`order` > 15")
    df.show()
    assert(df.count() == 2)
  }

  test("date literal pushdown") {
    val df = adbcReader.option("dbtable", "events").load()
      .filter("event_date > date '2024-03-01'")
    df.show()
    assert(df.count() == 2)
  }

  test("date literal IN list") {
    val df = adbcReader.option("dbtable", "events").load()
      .filter("event_date IN (date '2024-01-15', date '2025-03-10')")
    df.show()
    assert(df.count() == 2)
  }

  test("boolean literal pushdown") {
    val df = adbcReader.option("dbtable", "events").load()
    assume(df.schema("active").dataType == org.apache.spark.sql.types.BooleanType,
      "driver does not expose the column as boolean")
    val filtered = df.filter("active = true")
    filtered.show()
    assert(filtered.count() == 2)
  }

  test("topN nulls first (Spark default, ASC)") {
    val df = adbcReader.option("dbtable", "sortable").load()
      .orderBy(col("sort_key").asc).limit(3)
    val got = df.collect().map(r => Option(r.getAs[Any]("sort_key")).map(_.toString.toInt)).toSeq
    assert(got == Seq(None, None, Some(10)))
  }

  test("topN nulls last on ASC") {
    val df = adbcReader.option("dbtable", "sortable").load()
      .orderBy(col("sort_key").asc_nulls_last).limit(3)
    val got = df.collect().map(r => Option(r.getAs[Any]("sort_key")).map(_.toString.toInt)).toSeq
    assert(got == Seq(Some(10), Some(20), Some(30)))
  }

  test("topN nulls first on DESC") {
    val df = adbcReader.option("dbtable", "sortable").load()
      .orderBy(col("sort_key").desc_nulls_first).limit(3)
    val got = df.collect().map(r => Option(r.getAs[Any]("sort_key")).map(_.toString.toInt)).toSeq
    assert(got == Seq(None, None, Some(30)))
  }

  test("reserved keyword with order by") {
    val df = adbcReader.option("dbtable", "reserved_kw").load().orderBy("order").limit(2)
    df.show()
    assert(df.count() == 2)
    assert(df.collect().map(_.getAs[Int]("order")).toSeq == Seq(10, 20))
  }

  test("LIKE escape: wildcard in pattern is literal") {
    val s = spark
    import s.implicits._
    // Tornike contains literal 'T' but not 'i_'; the filter below must match
    // only rows where name contains literal 'i_' (which is zero rows). Without
    // proper escaping, 'i_' matches 'iX' for any X (2 rows: Tornike, Alice).
    val df = readTable.filter($"name".contains("i_"))
    assert(df.count() == 0)
  }

  test("single partition covers all rows") {
    val df = adbcReader
      .option("dbtable", "employees")
      .option("partitionColumn", "id")
      .option("lowerBound", "1")
      .option("upperBound", "4")
      .option("numPartitions", "1")
      .load()
    assert(df.count() == 3)
    assert(df.rdd.getNumPartitions == 1)
  }

  test("write appends rows and read back") {
    val s = spark
    import s.implicits._
    val batch = Seq(
      (10, "Newcomer", 5000),
      (11, "Fresh", 6000),
      (12, "Arrival", 7000)
    ).toDF("id", "name", "salary")
    writeTo(batch, "write_target")
    val got = adbcReader.option("dbtable", "write_target").load()
    assert(got.count() == 3)
    val rows = got.orderBy("id").collect()
    assert(rows.map(_.getAs[Int]("id")).toSeq == Seq(10, 11, 12))
    assert(rows.map(_.getAs[Int]("salary")).toSeq == Seq(5000, 6000, 7000))
  }

  test("probe: getInfo SqlInfo codes") {
    val tag = adbcParams.getOrElse("jni.driver", "?")
    val allocator = new RootAllocator(Long.MaxValue)
    val db = AdbcDriverManager.getInstance().connect(adbcDriver, allocator, adbcParams.asJava)
    try {
      val conn = db.connect()
      try {
        // Request specific SqlInfo codes: 504=SQL_IDENTIFIER_QUOTE_CHAR, 507=SQL_NULL_ORDERING,
        // 522=SQL_SUPPORTED_GROUP_BY, 509-512=functions, 503/505=case sensitivity.
        val codes = Array(503, 504, 505, 507, 508, 509, 510, 511, 512, 520, 521, 522, 523, 528)
        val reader = try conn.getInfo(codes) catch {
          case e: Throwable =>
            println(s"[info/$tag] getInfo(codes) FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}")
            null
        }
        if (reader != null) {
          try {
            var any = false
            while (reader.loadNextBatch()) {
              val root = reader.getVectorSchemaRoot
              val codeVec = root.getVector("info_name")
              val valVec = root.getVector("info_value")
              for (i <- 0 until root.getRowCount) {
                any = true
                println(s"[info/$tag]   code=${codeVec.getObject(i)} value=${valVec.getObject(i)}")
              }
            }
            if (!any) println(s"[info/$tag]   (no sql info codes returned)")
          } finally reader.close()
        }
      } finally conn.close()
    } finally db.close()
  }

  test("probe: executeSchema support") {
    val queries = Seq(
      "q1 select-star"       -> "SELECT * FROM employees",
      "q2 pure count"        -> "SELECT COUNT(*) FROM employees",
      "q3 sum only"          -> "SELECT SUM(salary) FROM employees",
      "q4 arithmetic"        -> "SELECT salary + 1 FROM employees",
      "q5 literal"           -> "SELECT 1 AS x FROM employees",
      "q6 string fn"         -> "SELECT UPPER(name) FROM employees",
      "q7 aliased col"       -> "SELECT salary AS pay FROM employees",
      "q8 aliased aggregate" -> "SELECT SUM(salary) AS total FROM employees",
      "q9 no from"           -> "SELECT 1 AS x"
    )
    val tag = adbcParams.getOrElse("jni.driver", "?")
    val allocator = new RootAllocator(Long.MaxValue)
    val db = AdbcDriverManager.getInstance().connect(adbcDriver, allocator, adbcParams.asJava)
    try {
      val conn = db.connect()
      try {
        queries.foreach { case (label, sql) =>
          val es = tryExecuteSchema(conn, sql)
          val wf = tryWhereFalseSchema(conn, sql)
          println(s"[probe/$tag] $label")
          println(s"  executeSchema: $es")
          println(s"  where 1=0   : $wf")
        }
      } finally conn.close()
    } finally db.close()
  }

  private def tryExecuteSchema(conn: org.apache.arrow.adbc.core.AdbcConnection, sql: String): String = {
    val stmt = conn.createStatement()
    try {
      stmt.setSqlQuery(sql)
      try { stmt.executeSchema().toString }
      catch { case e: Throwable => s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}" }
    } finally stmt.close()
  }

  private def tryWhereFalseSchema(conn: org.apache.arrow.adbc.core.AdbcConnection, sql: String): String = {
    val wrapped = s"SELECT * FROM ($sql) AS t WHERE 1=0"
    val stmt = conn.createStatement()
    try {
      stmt.setSqlQuery(wrapped)
      val result = try stmt.executeQuery() catch {
        case e: Throwable => return s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}"
      }
      try { result.getReader.getVectorSchemaRoot.getSchema.toString }
      catch { case e: Throwable => s"FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}" }
      finally result.close()
    } finally stmt.close()
  }

}
