package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class AdbcBatchWrite(schema: StructType, driver: String, params: Map[String, String], table: String) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new AdbcDataWriterFactory(schema, driver, params, table)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
