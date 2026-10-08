package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.util.control.NonFatal

class AdbcSinglestoreTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _
  private val database = "test"

  override protected def engine: String = "singlestore"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("ghcr.io/singlestore-labs/singlestoredb-dev:0.2.85"))
    container.withEnv("ROOT_PASSWORD", "test")
    container.withExposedPorts(3306)
    container.waitingFor(Wait.forListeningPort().withStartupTimeout(java.time.Duration.ofMinutes(5)))
    container.start()
    createDatabase()
  }

  // The image starts with no user database, and accepts connections a little before queries.
  private def createDatabase(): Unit = {
    val deadline = System.nanoTime() + 120L * 1000 * 1000 * 1000
    var ready = false
    while (!ready) {
      ready = try {
        withConnection(adbcParams + ("uri" -> uri("information_schema")))(execSetup(_, s"CREATE DATABASE IF NOT EXISTS $database"))
        true
      } catch {
        case NonFatal(e) if System.nanoTime() < deadline => Thread.sleep(1000); false
      }
    }
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  // The driver takes a Go MySQL DSN rather than a URL.
  private def uri(db: String): String = s"root:test@tcp(${container.getHost}:${container.getMappedPort(3306)})/$db"

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "singlestore",
    "uri" -> uri(database)
  )

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

  // Backslash is an escape character in string literals.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case s: String => super.setupLiteral(s.replace("\\", "\\\\"), t)
    // No offset is accepted in a date/time string; the session zone is UTC.
    case i: Instant => s"'${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)}'"
    case _ => super.setupLiteral(v, t)
  }

  override protected def knownGaps: Map[String, String] = Map(
    "types: decimals narrower than 64 bits" -> Gaps.NarrowDecimal,
    "write: all types round trip" -> Gaps.BooleanAsTinyint,
    "literal string: trailing space is significant" -> Gaps.Collation,
    "agg: group by string is case sensitive" -> Gaps.Collation,
    "agg: sum of integers" -> Gaps.WideDecimal,
    "agg: sum of decimal and double" -> Gaps.WideDecimal
  )

}
