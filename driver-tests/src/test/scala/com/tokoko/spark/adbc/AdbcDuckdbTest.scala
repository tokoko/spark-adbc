package com.tokoko.spark.adbc

import java.io.File

class AdbcDuckdbTest extends AdbcTestBase {

  private val dbFile = new File("test.duckdb")

  override protected def engine: String = "duckdb"

  override protected def startDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  override protected def stopDatabase(): Unit = {
    if (dbFile.exists()) dbFile.delete()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "duckdb",
    "path" -> dbFile.getAbsolutePath
  )

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "SMALLINT"
    case ColType.Int32 => "INTEGER"
    case ColType.Int64 => "BIGINT"
    case ColType.Float32 => "REAL"
    case ColType.Float64 => "DOUBLE"
    case ColType.Decimal(p, s) => s"DECIMAL($p, $s)"
    case ColType.Str => "VARCHAR"
    case ColType.Bool => "BOOLEAN"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "TIMESTAMP"
    case ColType.TimestampTz => "TIMESTAMPTZ"
    case ColType.Binary => "BLOB"
  })

  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case b: Array[Byte] => s"from_hex('${hex(b)}')"
    case _ => super.setupLiteral(v, t)
  }

}
