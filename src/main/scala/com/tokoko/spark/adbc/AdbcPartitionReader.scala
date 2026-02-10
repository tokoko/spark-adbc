package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.util.ArrowUtilsExtended

import collection.JavaConverters._

class AdbcPartitionReader(driver: String, params: Map[String, String], query: String) extends PartitionReader[InternalRow] {
  var queryExecuted = false
  var iterator: Iterator[InternalRow] = _
  var batchReader: ArrowReader = _

  override def next(): Boolean = {
    if (!queryExecuted) {
      val allocator = new RootAllocator(Long.MaxValue)
      val parameters: java.util.Map[String, Object] = params.mapValues(v => v: Object).asJava
      val database = AdbcDriverManager.getInstance()
        .connect(driver, allocator, parameters)

      val adbcConn = database.connect()

      val statement = adbcConn.createStatement()

      statement.setSqlQuery(query)
      batchReader = statement.executeQuery().getReader
      queryExecuted = true
    }

    if ((iterator == null || !iterator.hasNext) && batchReader.loadNextBatch()) {
      iterator = ArrowUtilsExtended.fromVectorSchemaRoot(batchReader.getVectorSchemaRoot)
    }

    iterator.hasNext
  }

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {}
}
