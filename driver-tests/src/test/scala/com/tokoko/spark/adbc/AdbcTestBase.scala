package com.tokoko.spark.adbc

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class AdbcTestBase extends AnyFunSuite with BeforeAndAfterAll {

  protected var spark: SparkSession = _

  protected def setupDatabase(): Unit
  protected def teardownDatabase(): Unit

  /** Returns a DataFrameReader pre-configured with driver-specific options. */
  protected def adbcReader: DataFrameReader

  protected def readTable: DataFrame =
    adbcReader.option("dbtable", "employees").load()

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

}
