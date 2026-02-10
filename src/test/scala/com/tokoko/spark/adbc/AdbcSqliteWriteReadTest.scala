package com.tokoko.spark.adbc

import org.apache.spark.sql.SparkSession
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
    stmt.close()
    conn.close()

    spark = SparkSession.builder().master("local").getOrCreate()
  }

  override def afterAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()
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
    assert(df.count() == 2)
    assert(df.columns.toSet == Set("Id", "Name", "Salary"))
  }

}
