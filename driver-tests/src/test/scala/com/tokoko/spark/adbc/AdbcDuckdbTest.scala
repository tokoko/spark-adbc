package com.tokoko.spark.adbc

import org.apache.spark.sql.DataFrameReader

import java.io.File
import java.sql.DriverManager

class AdbcDuckdbTest extends AdbcTestBase {

  private val dbFile = new File("test.duckdb")
  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"

  override protected def setupDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()

    val conn = DriverManager.getConnection(s"jdbc:duckdb:${dbFile.getAbsolutePath}")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE employees(id INTEGER NOT NULL, name VARCHAR, salary INTEGER NOT NULL)")
    stmt.execute("INSERT INTO employees VALUES (1, 'Tornike', 2000)")
    stmt.execute("INSERT INTO employees VALUES (2, 'Robin', 3000)")
    stmt.execute("INSERT INTO employees VALUES (3, 'Alice', 4000)")
    stmt.close()
    conn.close()
  }

  override protected def teardownDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  override protected def adbcReader: DataFrameReader = {
    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "duckdb")
      .option("path", dbFile.getAbsolutePath)
  }

}
