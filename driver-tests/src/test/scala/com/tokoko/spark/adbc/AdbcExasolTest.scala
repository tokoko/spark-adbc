package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import scala.util.control.NonFatal

class AdbcExasolTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _
  private val schema = "TEST"

  override protected def engine: String = "exasol"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("exasol/docker-db:2026.1.2"))
    container.withPrivilegedMode(true)
    container.withSharedMemorySize(2L * 1024 * 1024 * 1024)
    container.withExposedPorts(8563)
    container.waitingFor(Wait.forListeningPort().withStartupTimeout(java.time.Duration.ofMinutes(10)))
    container.start()
    createSchema()
  }

  // The port opens well before the database accepts logins.
  private def createSchema(): Unit = {
    val deadline = System.nanoTime() + 600L * 1000 * 1000 * 1000
    var ready = false
    while (!ready) {
      ready = try {
        withConnection(adbcParams + ("uri" -> uri(None)))(execSetup(_, s"CREATE SCHEMA IF NOT EXISTS $schema"))
        true
      } catch {
        case NonFatal(e) if System.nanoTime() < deadline => Thread.sleep(2000); false
      }
    }
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  private def uri(schema: Option[String]): String =
    s"exasol://sys:exasol@${container.getHost}:${container.getMappedPort(8563)}${schema.fold("")("/" + _)}" +
      "?validateservercertificate=0"

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "exasol",
    "uri" -> uri(Some(schema))
  )

  // Unquoted identifiers fold to upper case, and the fixture columns are created quoted.
  override protected def quoteProbeColumns: Boolean = true

  // Fixture tables are created unquoted, so their stored names are upper case.
  override protected def ingestTable(name: String): String = name.toUpperCase

  // Every integer type is a DECIMAL(n, 0) and FLOAT is a double. There is no binary type, and
  // TIMESTAMP WITH LOCAL TIME ZONE comes back as a zone-less timestamp in the session zone.
  override protected def sqlType(t: ColType): Option[String] = t match {
    case ColType.Int16 => Some("SMALLINT")
    case ColType.Int32 => Some("INTEGER")
    case ColType.Int64 => Some("BIGINT")
    case ColType.Float32 => Some("FLOAT")
    case ColType.Float64 => Some("DOUBLE PRECISION")
    case ColType.Decimal(p, s) => Some(s"DECIMAL($p, $s)")
    case ColType.Str => Some("VARCHAR(255)")
    case ColType.Bool => Some("BOOLEAN")
    case ColType.Date => Some("DATE")
    case ColType.Timestamp => Some("TIMESTAMP(6)")
    case ColType.TimestampTz | ColType.Binary => None
  }

}
