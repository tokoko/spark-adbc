package com.tokoko.spark.adbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.DriverManager

class AdbcSqliteWriteReadTest extends AnyFunSuite with BeforeAndAfterAll {

  private val dbFile = new File("test.sqlite")
  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()

    val conn = DriverManager.getConnection(s"jdbc:sqlite:${dbFile.getAbsolutePath}")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE Employees(Id INTEGER NOT NULL, Name TEXT, Salary INTEGER NOT NULL)")
    stmt.execute("INSERT INTO Employees VALUES (1, 'Tornike', 2000)")
    stmt.execute("INSERT INTO Employees VALUES (2, 'Robin', 3000)")
    stmt.execute("INSERT INTO Employees VALUES (3, 'Alice', 4000)")
    stmt.close()
    conn.close()

    spark = SparkSession.builder().master("local").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  private def readTable = {
    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "sqlite")
      .option("uri", dbFile.getAbsolutePath)
      .option("dbtable", "Employees")
      .load()
  }

  test("read via ADBC SQLite JNI driver") {
    val df = spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "sqlite")
      .option("uri", dbFile.getAbsolutePath)
      .option("query", "SELECT * FROM Employees")
      .load()

    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("Id", "Name", "Salary"))
  }

  test("read via dbtable option") {
    val df = readTable
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("Id", "Name", "Salary"))
  }

  test("column pruning") {
    val df = readTable.select("Name", "Salary")
    df.show()
    assert(df.count() == 3)
    assert(df.columns.toSet == Set("Name", "Salary"))
  }

  test("filter pushdown - equality") {
    val df = readTable.filter("Name = 'Robin'")
    df.show()
    assert(df.count() == 1)
    assert(df.collect()(0).getAs[String]("Name") == "Robin")
  }

  test("filter pushdown - comparison") {
    val df = readTable.filter("Salary > 2000")
    df.show()
    assert(df.count() == 2)
  }

  test("filter pushdown with column pruning") {
    val df = readTable.select("Name", "Salary").filter("Salary >= 3000")
    df.show()
    assert(df.count() == 2)
    assert(df.columns.toSet == Set("Name", "Salary"))
  }

  test("limit pushdown") {
    val df = readTable.limit(2)
    df.show()
    assert(df.count() == 2)
  }

  test("topN pushdown - descending") {
    val df = readTable.orderBy(col("Salary").desc).limit(1)
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getAs[String]("Name") == "Alice")
  }

  test("topN pushdown - ascending") {
    val df = readTable.orderBy(col("Salary").asc).limit(2)
    df.show()
    val rows = df.collect()
    assert(rows.length == 2)
    assert(rows(0).getAs[String]("Name") == "Tornike")
    assert(rows(1).getAs[String]("Name") == "Robin")
  }

  test("topN pushdown with filter and column pruning") {
    val df = readTable
      .select("Name", "Salary")
      .filter("Salary > 2000")
      .orderBy(col("Salary").desc)
      .limit(1)
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getAs[String]("Name") == "Alice")
    assert(df.columns.toSet == Set("Name", "Salary"))
  }

  test("aggregate pushdown - count") {
    val df = readTable.agg(count("*"))
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getLong(0) == 3)
  }

  test("aggregate pushdown - sum, min, max") {
    val df = readTable.agg(sum("Salary"), min("Salary"), max("Salary"))
    df.show()
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getLong(0) == 9000) // sum
    assert(rows(0).getLong(1) == 2000) // min
    assert(rows(0).getLong(2) == 4000) // max
  }

  test("aggregate pushdown - group by") {
    val df = readTable.groupBy("Name").agg(sum("Salary").as("total"))
    df.show()
    assert(df.count() == 3)
  }

  test("aggregate pushdown - group by with filter") {
    val df = readTable.filter("Salary > 2000").groupBy("Name").agg(sum("Salary").as("total"))
    df.show()
    assert(df.count() == 2)
  }

}
