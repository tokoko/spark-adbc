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
    stmt.execute("""CREATE TABLE reserved_kw(id INTEGER NOT NULL, "order" INTEGER NOT NULL)""")
    stmt.execute("""INSERT INTO reserved_kw VALUES (1, 10), (2, 20), (3, 30)""")
    stmt.execute("CREATE TABLE events(id INTEGER NOT NULL, event_date DATE NOT NULL, active BOOLEAN NOT NULL)")
    stmt.execute("INSERT INTO events VALUES (1, DATE '2024-01-15', TRUE), (2, DATE '2024-06-20', FALSE), (3, DATE '2025-03-10', TRUE)")
    stmt.execute("CREATE TABLE sortable(id INTEGER NOT NULL, sort_key INTEGER)")
    stmt.execute("INSERT INTO sortable VALUES (1, 10), (2, NULL), (3, 30), (4, 20), (5, NULL)")
    stmt.execute("CREATE TABLE write_target(id INTEGER NOT NULL, name VARCHAR, salary INTEGER NOT NULL)")
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

  override protected def adbcDriver: String = driverFactory
  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "duckdb",
    "path" -> dbFile.getAbsolutePath
  )

}
