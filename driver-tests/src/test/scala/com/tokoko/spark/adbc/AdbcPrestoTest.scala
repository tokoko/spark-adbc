package com.tokoko.spark.adbc

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.LocalDate
import scala.util.control.NonFatal

/** Presto with its in-memory connector. */
class AdbcPrestoTest extends AdbcTestBase {

  private var container: GenericContainer[_] = _

  override protected def engine: String = "presto"

  override protected def startDatabase(): Unit = {
    container = new GenericContainer(DockerImageName.parse("prestodb/presto:0.299"))
    container.withExposedPorts(8080)
    container.waitingFor(Wait.forHttp("/v1/info").forPort(8080).forResponsePredicate(_.contains("\"starting\":false")))
    container.start()
    awaitWorker()
  }

  // The server answers before it can schedule table scans.
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
    "jni.driver" -> "presto",
    "uri" -> s"http://test@${container.getHost}:${container.getMappedPort(8080)}/memory/default"
  )

  // Presto timestamps are millisecond precision; the fixtures need microseconds, so the
  // timestamp columns are left out rather than compared after truncation.
  override protected def sqlType(t: ColType): Option[String] = t match {
    case ColType.Int16 => Some("smallint")
    case ColType.Int32 => Some("integer")
    case ColType.Int64 => Some("bigint")
    case ColType.Float32 => Some("real")
    case ColType.Float64 => Some("double")
    case ColType.Decimal(p, s) => Some(s"decimal($p, $s)")
    case ColType.Str => Some("varchar")
    case ColType.Bool => Some("boolean")
    case ColType.Date => Some("date")
    case ColType.Timestamp | ColType.TimestampTz => None
    case ColType.Binary => Some("varbinary")
  }

  // The memory connector has no NOT NULL columns.
  override protected def columnDdl(c: Col, nativeType: String): String = s"${quoteId(c.name)} $nativeType"

  // Presto doesn't coerce untyped literals on INSERT, so everything is typed explicitly.
  override protected def setupLiteral(v: Any, t: ColType): String = v match {
    case null => "NULL"
    case d: LocalDate => s"DATE '$d'"
    case _: String | _: Boolean | _: Array[Byte] => super.setupLiteral(v, t)
    case _ => s"CAST(${super.setupLiteral(v, t)} AS ${sqlType(t).get})"
  }

  override protected def knownGaps: Map[String, String] = Map(
    "types: 64-bit integers beyond double precision" -> Gaps.BigintViaDouble,
    "literal bigint: upper bound" -> Gaps.BigintViaDouble,
    "write: all types round trip" -> Gaps.IngestTypes
  )

}
