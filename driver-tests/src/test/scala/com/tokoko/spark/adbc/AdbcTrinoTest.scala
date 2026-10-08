package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import scala.util.control.NonFatal

/** Trino with its in-memory connector, which is enough to exercise Trino's SQL dialect. */
class AdbcTrinoTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "trino"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("trinodb/trino:476"))
    container.withExposedPorts(8080)
    container.waitingFor(Wait.forHttp("/v1/info").forPort(8080).forResponsePredicate(_.contains("\"starting\":false")))
    container.start()
    awaitWorker()
  }

  // The server answers before it can schedule table scans ("No nodes available to run query").
  private def awaitWorker(): Unit = {
    val deadline = System.nanoTime() + 120L * 1000 * 1000 * 1000
    var ready = false
    while (!ready) {
      ready = try {
        withConnection { conn =>
          val stmt = conn.createStatement()
          try {
            stmt.setSqlQuery("SELECT count(*) FROM tpch.tiny.nation")
            val result = stmt.executeQuery()
            try { while (result.getReader.loadNextBatch()) {}; true } finally result.close()
          } finally stmt.close()
        }
      } catch {
        case NonFatal(e) if System.nanoTime() < deadline => Thread.sleep(1000); false
      }
    }
  }

  override protected def stopDatabase(): Unit = {
    if (container != null) container.stop()
  }

  override protected def adbcParams: Map[String, Object] = Map(
    "jni.driver" -> "trino",
    "uri" -> s"http://test@${container.getHost}:${container.getMappedPort(8080)}?catalog=memory&schema=default"
  )

  override protected def sqlType(t: ColType): Option[String] = Some(t match {
    case ColType.Int16 => "smallint"
    case ColType.Int32 => "integer"
    case ColType.Int64 => "bigint"
    case ColType.Float32 => "real"
    case ColType.Float64 => "double"
    // The driver returns precision <= 18 as decimal32/decimal64, which Arrow Java misreads
    // (see the small_decimals test), so the general-purpose fixture column is made wider.
    case ColType.Decimal(18, s) => s"decimal(20, $s)"
    case ColType.Decimal(p, s) => s"decimal($p, $s)"
    case ColType.Str => "varchar"
    case ColType.Bool => "boolean"
    case ColType.Date => "date"
    case ColType.Timestamp => "timestamp(6)"
    case ColType.TimestampTz => "timestamp(6) with time zone"
    case ColType.Binary => "varbinary"
  })

  // Trino doesn't coerce untyped literals on INSERT, so everything is typed explicitly.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case null => "NULL"
    case d: LocalDate => s"DATE '$d'"
    case ts: LocalDateTime => s"TIMESTAMP '${ts.format(setupTimestampFormat)}'"
    case i: Instant => s"TIMESTAMP '${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)} UTC'"
    case _: String | _: Boolean | _: Array[Byte] => super.setupLiteral(v, t)
    case _ => s"CAST(${super.setupLiteral(v, t)} AS ${sqlType(t).get})"
  }

  override protected def knownGaps: Map[String, String] = Map(
    "types: decimals narrower than 64 bits" -> Gaps.NarrowDecimal
  )

}
