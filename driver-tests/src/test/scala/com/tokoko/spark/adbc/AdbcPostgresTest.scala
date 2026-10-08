package com.tokoko.spark.adbc

import org.testcontainers.containers.PostgreSQLContainer

class AdbcPostgresTest extends AdbcTestBase {

  private var container: PostgreSQLContainer[_] = _

  override protected def engine: String = "postgresql"

  override protected def startDatabase(): Unit = {
    container = new PostgreSQLContainer("postgres:17")
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = {
    val host = container.getHost
    val port = container.getMappedPort(5432)
    val db = container.getDatabaseName
    val user = container.getUsername
    val pass = container.getPassword
    Map(
      "jni.driver" -> "postgresql",
      "uri" -> s"postgresql://$user:$pass@$host:$port/$db"
    )
  }

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "SMALLINT"
    case ColType.Int32 => "INTEGER"
    case ColType.Int64 => "BIGINT"
    case ColType.Float32 => "REAL"
    case ColType.Float64 => "DOUBLE PRECISION"
    case ColType.Decimal(p, s) => s"NUMERIC($p, $s)"
    case ColType.Str => "TEXT"
    case ColType.Bool => "BOOLEAN"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "TIMESTAMP"
    case ColType.TimestampTz => "TIMESTAMPTZ"
    case ColType.Binary => "BYTEA"
  })

  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case b: Array[Byte] => s"'\\x${hex(b)}'::bytea"
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "write: all types round trip" -> Gaps.IngestTypes,
    "literal string: range comparison" -> Gaps.SortCollation,
    "topN: string order matches Spark" -> Gaps.SortCollation
  )

}
