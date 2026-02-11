package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

import scala.jdk.CollectionConverters._

class AdbcPartitionReader(driver: String, params: Map[String, String], query: String) extends PartitionReader[ColumnarBatch] {
  private var queryExecuted = false
  private var batchReader: ArrowReader = _
  private var currentBatch: ColumnarBatch = _

  override def next(): Boolean = {
    if (!queryExecuted) {
      val allocator = new RootAllocator(Long.MaxValue)
      val parameters: java.util.Map[String, Object] = params.view.mapValues(v => v: Object).toMap.asJava
      val database = AdbcDriverManager.getInstance()
        .connect(driver, allocator, parameters)

      val adbcConn = database.connect()

      val statement = adbcConn.createStatement()

      statement.setSqlQuery(query)
      batchReader = statement.executeQuery().getReader
      queryExecuted = true
    }

    if (batchReader.loadNextBatch()) {
      val root = batchReader.getVectorSchemaRoot
      val columns = root.getFieldVectors.asScala.map { v =>
        new ArrowColumnVector(v).asInstanceOf[ColumnVector]
      }.toArray
      currentBatch = new ColumnarBatch(columns, root.getRowCount)
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {}
}
