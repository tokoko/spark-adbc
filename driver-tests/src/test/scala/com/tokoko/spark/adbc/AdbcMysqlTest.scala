package com.tokoko.spark.adbc

import org.testcontainers.containers.MySQLContainer

class AdbcMysqlTest extends AdbcTestBase {

  private var container: MySQLContainer[_] = _

  override protected def engine: String = "mysql"

  override protected def startDatabase(): Unit = {
    container = new MySQLContainer("mysql:8.4")
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

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

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "SMALLINT"
    case ColType.Int32 => "INTEGER"
    case ColType.Int64 => "BIGINT"
    case ColType.Float32 => "FLOAT"
    case ColType.Float64 => "DOUBLE"
    // The driver returns precision <= 18 as decimal32/decimal64, which Arrow Java misreads
    // (see the small_decimals test), so the general-purpose fixture column is made wider.
    case ColType.Decimal(18, s) => s"DECIMAL(20, $s)"
    case ColType.Decimal(p, s) => s"DECIMAL($p, $s)"
    case ColType.Str => "VARCHAR(255)"
    case ColType.Bool => "BOOLEAN"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "DATETIME(6)"
    case ColType.TimestampTz => "TIMESTAMP(6)"
    case ColType.Binary => "VARBINARY(255)"
  })

  // Backslash is an escape character in MySQL string literals.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case s: String => super.setupLiteral(s.replace("\\", "\\\\"), t)
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "types: decimals narrower than 64 bits" -> Gaps.NarrowDecimal,
    "write: all types round trip" -> Gaps.BooleanAsTinyint,
    "literal string: equality" -> Gaps.Collation,
    "literal string: equality is case sensitive" -> Gaps.Collation,
    "literal string: range comparison" -> Gaps.Collation,
    "LIKE: prefix is case sensitive" -> Gaps.Collation,
    "agg: group by string is case sensitive" -> Gaps.Collation,
    "topN: string order matches Spark" -> Gaps.SortCollation,
    "agg: sum of integers" -> Gaps.WideDecimal,
    "agg: sum of decimal and double" -> Gaps.WideDecimal
  )

}
