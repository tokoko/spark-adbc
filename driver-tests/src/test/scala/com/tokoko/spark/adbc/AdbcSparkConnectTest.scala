package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

/** A Spark Connect server as the remote database, reached through the Spark ADBC driver. */
class AdbcSparkConnectTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "spark"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("apache/spark:4.1.1"))
    container.withEnv("SPARK_NO_DAEMONIZE", "true")
    container.withCommand("/opt/spark/sbin/start-connect-server.sh")
    container.withExposedPorts(15002)
    container.waitingFor(Wait.forLogMessage(".*Spark Connect server started.*", 1))
    container.start()
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "spark",
    "uri" -> s"spark://${container.getHost}:${container.getMappedPort(15002)}?api=connect&auth_type=none"
  )

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "SMALLINT"
    case ColType.Int32 => "INT"
    case ColType.Int64 => "BIGINT"
    case ColType.Float32 => "FLOAT"
    case ColType.Float64 => "DOUBLE"
    case ColType.Decimal(p, s) => s"DECIMAL($p, $s)"
    case ColType.Str => "STRING"
    case ColType.Bool => "BOOLEAN"
    case ColType.Date => "DATE"
    case ColType.Timestamp => "TIMESTAMP_NTZ"
    case ColType.TimestampTz => "TIMESTAMP"
    case ColType.Binary => "BINARY"
  })

  // The driver ingests by uploading files to a staging area (spark.ingest.staging_area_uri)
  // that the server reads back, i.e. shared object storage, which this setup doesn't have.
  override protected def supportsWrite: Boolean = false

  override protected def createTableSuffix: String = " USING parquet"

  // ANSI store assignment won't turn strings into dates or narrow numeric literals, and
  // backslash is an escape character in string literals.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case null => "NULL"
    case s: String => super.setupLiteral(s.replace("\\", "\\\\"), t)
    case d: LocalDate => s"DATE '$d'"
    case ts: LocalDateTime => s"TIMESTAMP_NTZ '${ts.format(setupTimestampFormat)}'"
    case i: Instant => s"TIMESTAMP '${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)}+00:00'"
    case _: Boolean | _: Array[Byte] => super.setupLiteral(v, t)
    case _ => s"CAST(${super.setupLiteral(v, t)} AS ${sqlType(t).get})"
  }

}
