package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

/**
 * Oracle Database Free, through the open-source driver from
 * https://github.com/gizmodata/adbc-driver-oracle (the `oracle` driver in the dbc registry is
 * private). That driver isn't installed by dbc; see CLAUDE.md for the manifest it needs.
 */
class AdbcOracleTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "oracle"

  // The driver is registered under its own name, so the dialect is given explicitly.
  override protected def dialectName: Option[String] = Some("oracle")

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart"))
    container.withEnv("ORACLE_PASSWORD", "tiger")
    container.withEnv("APP_USER", "scott")
    container.withEnv("APP_USER_PASSWORD", "tiger")
    container.withExposedPorts(1521)
    container.waitingFor(
      Wait.forLogMessage(".*DATABASE IS READY TO USE!.*", 1).withStartupTimeout(java.time.Duration.ofMinutes(5)))
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  // Oracle's DATE carries a time of day, which the driver returns as a second-precision
  // timestamp that Spark has no mapping for; date_mode=date32 returns plain dates instead.
  //
  // number_mode stays on its default. Computed NUMBERs (any aggregate) then come back as
  // doubles, which is inexact beyond 15 digits; number_mode=decimal would be the fix, but in
  // driver 1.0.0 it rounds those values to whole numbers (AVG of 1 and 2 returns 2).
  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "oracle-gizmodata",
    "uri" -> s"oracle://scott:tiger@${container.getHost}:${container.getMappedPort(1521)}/FREEPDB1?date_mode=date32"
  )

  // Unquoted identifiers fold to upper case, and the fixture columns are created quoted.
  override protected def quoteProbeColumns: Boolean = true

  // Fixture tables are created unquoted, so their stored names are upper case.
  override protected def ingestTable(name: String): String = name.toUpperCase

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "NUMBER(5)"
    case ColType.Int32 => "NUMBER(10)"
    case ColType.Int64 => "NUMBER(19)"
    case ColType.Float32 => "BINARY_FLOAT"
    case ColType.Float64 => "BINARY_DOUBLE"
    case ColType.Decimal(p, s) => s"NUMBER($p, $s)"
    case ColType.Str => "VARCHAR2(255)"
    case ColType.Bool => "BOOLEAN"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "TIMESTAMP(6)"
    case ColType.TimestampTz => "TIMESTAMP(6) WITH TIME ZONE"
    case ColType.Binary => "RAW(255)"
  })

  // Bare strings are converted through the session's NLS formats, so dates and timestamps
  // are written as typed literals.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case d: LocalDate => s"DATE '$d'"
    case ts: LocalDateTime => s"TIMESTAMP '${ts.format(setupTimestampFormat)}'"
    case i: Instant => s"TIMESTAMP '${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)} +00:00'"
    case b: Array[Byte] => s"HEXTORAW('${hex(b)}')"
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "agg: min and max of decimal" -> Gaps.AggregateAsDouble,
    "write appends rows and read back" -> Gaps.IngestColumnCase,
    "write: all types round trip" -> Gaps.IngestColumnCase
  )

}
