package com.tokoko.spark.adbc

import org.apache.spark.sql.DataFrameReader
import org.testcontainers.containers.MySQLContainer

import java.sql.DriverManager

class AdbcMysqlTest extends AdbcTestBase {

  private val driverFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"
  private var container: MySQLContainer[_] = _

  override protected def setupDatabase(): Unit = {
    container = new MySQLContainer("mysql:8.4")
    container.start()

    val conn = DriverManager.getConnection(
      container.getJdbcUrl, container.getUsername, container.getPassword
    )
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE employees(id INTEGER NOT NULL, name VARCHAR(255), salary INTEGER NOT NULL)")
    stmt.execute("INSERT INTO employees VALUES (1, 'Tornike', 2000)")
    stmt.execute("INSERT INTO employees VALUES (2, 'Robin', 3000)")
    stmt.execute("INSERT INTO employees VALUES (3, 'Alice', 4000)")
    stmt.execute("CREATE TABLE reserved_kw(id INTEGER NOT NULL, `order` INTEGER NOT NULL)")
    stmt.execute("INSERT INTO reserved_kw VALUES (1, 10), (2, 20), (3, 30)")
    stmt.execute("CREATE TABLE events(id INTEGER NOT NULL, event_date DATE NOT NULL, active BOOLEAN NOT NULL)")
    stmt.execute("INSERT INTO events VALUES (1, '2024-01-15', TRUE), (2, '2024-06-20', FALSE), (3, '2025-03-10', TRUE)")
    stmt.execute("CREATE TABLE sortable(id INTEGER NOT NULL, sort_key INTEGER)")
    stmt.execute("INSERT INTO sortable VALUES (1, 10), (2, NULL), (3, 30), (4, 20), (5, NULL)")
    stmt.execute("CREATE TABLE write_target(id INTEGER NOT NULL, name VARCHAR(255), salary INTEGER NOT NULL)")
    stmt.close()
    conn.close()
  }

  override protected def teardownDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcReader: DataFrameReader = {
    val host = container.getHost
    val port = container.getMappedPort(3306)
    val db = container.getDatabaseName
    val user = container.getUsername
    val pass = container.getPassword
    val uri = s"mysql://$user:$pass@$host:$port/$db"

    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driverFactory)
      .option("jni.driver", "mysql")
      .option("uri", uri)
  }

  override protected def adbcDriver: String = driverFactory
  override protected def adbcParams: Map[String, Object] = {
    val host = container.getHost
    val port = container.getMappedPort(3306)
    val db = container.getDatabaseName
    val user = container.getUsername
    val pass = container.getPassword
    Map(
      "jni.driver" -> "mysql",
      "uri" -> s"mysql://$user:$pass@$host:$port/$db"
    )
  }

}
