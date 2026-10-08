package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Instant, LocalDateTime, ZoneOffset}

class AdbcClickhouseTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "clickhouse"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("clickhouse/clickhouse-server:25.8"))
    container.withEnv("CLICKHOUSE_USER", "test")
    container.withEnv("CLICKHOUSE_PASSWORD", "test")
    container.withExposedPorts(8123)
    container.waitingFor(Wait.forHttp("/ping").forPort(8123))
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "clickhouse",
    "uri" -> s"http://${container.getHost}:${container.getMappedPort(8123)}",
    "username" -> "test",
    "password" -> "test"
  )

  // ClickHouse has no binary type distinct from String.
  override protected def sqlType(t: ColType): Option[String] = t match {
    case ColType.Int16 => Some("Int16")
    case ColType.Int32 => Some("Int32")
    case ColType.Int64 => Some("Int64")
    case ColType.Float32 => Some("Float32")
    case ColType.Float64 => Some("Float64")
    case ColType.Decimal(p, s) => Some(s"Decimal($p, $s)")
    case ColType.Str => Some("String")
    case ColType.Bool => Some("Bool")
    case ColType.Date => Some("Date32")
    case ColType.Timestamp => Some("DateTime64(6)")
    case ColType.TimestampTz => Some("DateTime64(6, 'UTC')")
    case ColType.Binary => None
  }

  override protected def columnDdl(c: Col, nativeType: String): String =
    s"${quoteId(c.name)} " + (if (c.nullable) s"Nullable($nativeType)" else nativeType)

  override protected def createTableSuffix: String = " ENGINE = Memory"

  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case s: String => super.setupLiteral(s.replace("\\", "\\\\"), t)
    case i: Instant => s"'${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)}'"
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "aggregate pushdown - count" -> Gaps.UnsignedCount,
    "agg: count star" -> Gaps.UnsignedCount,
    "agg: count column skips nulls" -> Gaps.UnsignedCount,
    "agg: count distinct" -> Gaps.UnsignedCount,
    "agg: group by boolean" -> Gaps.UnsignedCount,
    "agg: group by nullable key" -> Gaps.UnsignedCount,
    "agg: group by date" -> Gaps.UnsignedCount,
    "agg: group by string is case sensitive" -> Gaps.UnsignedCount,
    "agg: over empty input" -> Gaps.UnsignedCount,
    "agg: with filter" -> Gaps.UnsignedCount,
    "agg: over query option" -> Gaps.UnsignedCount
  )

}
