package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcException, AdbcStatusCode, BulkIngestMode}
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExtended

import scala.jdk.CollectionConverters._

class AdbcDataWriter(schema: StructType, driver: String, params: Map[String, String], table: String, timeZone: String)
  extends DataWriter[InternalRow] {

  private val allocator = new RootAllocator(Long.MaxValue)
  private var database: AdbcDatabase = _
  private var conn: AdbcConnection = _
  private var transactional: Boolean = false
  private var root: VectorSchemaRoot = _
  private var arrowWriter: ArrowWriter = _
  private var committed: Boolean = false

  try {
    val parameters: java.util.Map[String, Object] =
      params.view.mapValues(v => v: Object).toMap.asJava
    database = AdbcDriverManager.getInstance().connect(driver, allocator, parameters)
    conn = database.connect()
    transactional = try {
      conn.setAutoCommit(false)
      true
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => false
    }
    val arrowSchema = ArrowUtilsExtended.toArrowSchema(schema, timeZone)
    root = VectorSchemaRoot.create(arrowSchema, allocator)
    arrowWriter = ArrowWriter.create(root)
  } catch {
    case e: Throwable =>
      closeQuietly()
      throw e
  }

  override def write(record: InternalRow): Unit = {
    arrowWriter.write(record)
  }

  override def commit(): WriterCommitMessage = {
    arrowWriter.finish()
    val stmt = conn.bulkIngest(table, BulkIngestMode.APPEND)
    try {
      stmt.bind(root)
      stmt.executeUpdate()
      if (transactional) conn.commit()
      committed = true
    } finally stmt.close()
    new AdbcWriterCommitMessage
  }

  override def abort(): Unit = {
    if (transactional && !committed) {
      try conn.rollback() catch { case _: AdbcException => /* best effort */ }
    }
  }

  override def close(): Unit = closeQuietly()

  private def closeQuietly(): Unit = {
    def safe[A](f: => A): Unit = try { f; () } catch { case _: Throwable => () }
    if (root != null) safe(root.close())
    if (conn != null) safe(conn.close())
    if (database != null) safe(database.close())
    safe(allocator.close())
  }
}
