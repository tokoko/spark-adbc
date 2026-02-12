package com.tokoko.spark.adbc

import org.apache.spark.sql.DataFrameReader
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.DriverManager

class AdbcPostgresTest extends AdbcTestBase {

  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"
  private var container: PostgreSQLContainer[_] = _

  override protected def setupDatabase(): Unit = {
    container = new PostgreSQLContainer("postgres:17")
    container.start()

    val conn = DriverManager.getConnection(
      container.getJdbcUrl, container.getUsername, container.getPassword
    )
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE employees(id INTEGER NOT NULL, name TEXT, salary INTEGER NOT NULL)")
    stmt.execute("INSERT INTO employees VALUES (1, 'Tornike', 2000)")
    stmt.execute("INSERT INTO employees VALUES (2, 'Robin', 3000)")
    stmt.execute("INSERT INTO employees VALUES (3, 'Alice', 4000)")
    stmt.close()
    conn.close()
  }

  override protected def teardownDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcReader: DataFrameReader = {
    val host = container.getHost
    val port = container.getMappedPort(5432)
    val db = container.getDatabaseName
    val user = container.getUsername
    val pass = container.getPassword
    val uri = s"postgresql://$user:$pass@$host:$port/$db"

    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "postgresql")
      .option("uri", uri)
  }

}
