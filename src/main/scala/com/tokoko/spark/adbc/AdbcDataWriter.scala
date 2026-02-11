package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExtended

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class AdbcDataWriter(schema: StructType, driver: String, params: Map[String, String], table: String) extends DataWriter[InternalRow] {
  // TODO more efficient solution???
  private val rowList = mutable.ListBuffer.empty[InternalRow]

  override def write(record: InternalRow): Unit = {
    rowList.append(record)
  }

  override def commit(): WriterCommitMessage = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = ArrowUtilsExtended.toArrowSchema(schema, "UTC")
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    rowList.foreach(row => arrowWriter.write(row))
    arrowWriter.finish()

    val parameters: java.util.Map[String, Object] = params.view.mapValues(v => v: Object).toMap.asJava
    val database = AdbcDriverManager.getInstance()
      .connect(driver, allocator, parameters)

    val adbcConn = database.connect()

    val ingestStatement = adbcConn.bulkIngest(table, BulkIngestMode.APPEND)
    ingestStatement.bind(root)
    ingestStatement.executeUpdate()

    new AdbcWriterCommitMessage
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
