package com.tokoko.spark.adbc

import org.testcontainers.containers.MSSQLServerContainer

class AdbcMssqlTest extends AdbcTestBase {

  private var container: MSSQLServerContainer[_] = _

  override protected def engine: String = "mssql"

  override protected def startDatabase(): Unit = {
    container = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest")
    container.acceptLicense()
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = {
    val host = container.getHost
    val port = container.getMappedPort(1433)
    val user = container.getUsername
    val pass = container.getPassword
    Map(
      "jni.driver" -> "mssql",
      "uri" -> s"mssql://$user:$pass@$host:$port?database=master"
    )
  }

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "SMALLINT"
    case ColType.Int32 => "INTEGER"
    case ColType.Int64 => "BIGINT"
    case ColType.Float32 => "REAL"
    case ColType.Float64 => "FLOAT"
    case ColType.Decimal(p, s) => s"DECIMAL($p, $s)"
    case ColType.Str => "NVARCHAR(255)"
    case ColType.Bool => "BIT"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "DATETIME2(6)"
    case ColType.TimestampTz => "DATETIMEOFFSET(6)"
    case ColType.Binary => "VARBINARY(255)"
  })

  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case b: Boolean => if (b) "1" else "0"
    case s: String => "N" + super.setupLiteral(s, t)
    case b: Array[Byte] => s"0x${hex(b)}"
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "literal string: equality" -> Gaps.Collation,
    "literal string: equality is case sensitive" -> Gaps.Collation,
    "literal string: trailing space is significant" -> Gaps.Collation,
    "literal string: range comparison" -> Gaps.Collation,
    "LIKE: prefix is case sensitive" -> Gaps.Collation,
    "agg: group by string is case sensitive" -> Gaps.Collation,
    "topN: string order matches Spark" -> Gaps.SortCollation,
    "agg: sum(int) beyond the int range" -> Gaps.IntSumOverflow,
    "agg: avg of int keeps the fraction" -> Gaps.IntegerAvg
  )

}
