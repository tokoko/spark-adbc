package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

// At-least-once semantics: each task commits its own transaction in
// `AdbcDataWriter.commit`. There is no cross-task atomicity — if some tasks
// succeed and others fail, the successful ones stay committed. ADBC does not
// expose a distributed-write coordination primitive to do better.
class AdbcBatchWrite(schema: StructType, driver: String, params: Map[String, String], table: String, timeZone: String) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new AdbcDataWriterFactory(schema, driver, params, table, timeZone)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
