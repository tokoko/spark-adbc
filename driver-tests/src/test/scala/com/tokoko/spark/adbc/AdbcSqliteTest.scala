package com.tokoko.spark.adbc

import java.io.File

/**
 * Syntax probes only. The SQLite driver infers column types from the rows it returns, so the
 * connector's zero-row schema probe sees every column as int64 and reads through Spark don't
 * work; the dialect itself is still worth having in the matrix.
 */
class AdbcSqliteTest extends AdbcSuiteBase with DialectProbeTests {

  private val dbFile = new File("test.sqlite")

  override protected def engine: String = "sqlite"

  override protected def startDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  override protected def stopDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "sqlite",
    "uri" -> dbFile.getAbsolutePath
  )

  // Dates and timestamps are stored as ISO 8601 text; there is no time zone aware type.
  override protected def sqlType(t: ColType): Option[String] = t match {
    case ColType.Int16 | ColType.Int32 | ColType.Int64 => Some("INTEGER")
    case ColType.Float32 | ColType.Float64 => Some("REAL")
    case ColType.Decimal(p, s) => Some(s"DECIMAL($p, $s)")
    case ColType.Str => Some("TEXT")
    case ColType.Bool => Some("BOOLEAN")
    case ColType.Date => Some("DATE")
    case ColType.Timestamp => Some("TIMESTAMP")
    case ColType.TimestampTz => None
    case ColType.Binary => Some("BLOB")
  }

  override protected def knownGaps: Map[String, String] = Map(
    "dialect: configured date/time literal is accepted" -> "the connector has no SQLite dialect: dates are ISO text and DATE '...' is rejected"
  )

}
