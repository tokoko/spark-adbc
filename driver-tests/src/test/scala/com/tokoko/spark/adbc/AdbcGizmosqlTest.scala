package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * GizmoSQL is DuckDB behind a Flight SQL server, so the SQL is already covered by the DuckDB
 * suite; what this adds is the Flight SQL driver as the transport.
 */
class AdbcGizmosqlTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "gizmosql"

  override protected def dialectName: Option[String] = Some("default")

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("gizmodata/gizmosql:v1.41.0-slim"))
    container.withEnv("GIZMOSQL_USERNAME", "test")
    container.withEnv("GIZMOSQL_PASSWORD", "test")
    container.withEnv("TLS_ENABLED", "0")
    container.withExposedPorts(31337)
    container.waitingFor(Wait.forLogMessage(".*GizmoSQL server - started.*", 1))
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "flightsql",
    "uri" -> s"grpc://${container.getHost}:${container.getMappedPort(31337)}",
    "username" -> "test",
    "password" -> "test"
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
