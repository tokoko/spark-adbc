package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

import scala.jdk.CollectionConverters._

class AdbcPartitionReader(driver: String, params: Map[String, String], query: String) extends PartitionReader[ColumnarBatch] {
  private var queryExecuted = false
  private var allocator: RootAllocator = _
  private var database: org.apache.arrow.adbc.core.AdbcDatabase = _
  private var adbcConn: org.apache.arrow.adbc.core.AdbcConnection = _
  private var statement: org.apache.arrow.adbc.core.AdbcStatement = _
  private var batchReader: ArrowReader = _
  private var currentBatch: ColumnarBatch = _

  override def next(): Boolean = {
    if (!queryExecuted) {
      allocator = new RootAllocator(Long.MaxValue)
      val parameters: java.util.Map[String, Object] = params.view.mapValues(v => v: Object).toMap.asJava
      database = AdbcDriverManager.getInstance()
        .connect(driver, allocator, parameters)

      adbcConn = database.connect()

      statement = adbcConn.createStatement()

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

  override def close(): Unit = {
    if (batchReader != null) batchReader.close()
    if (statement != null) statement.close()
    if (adbcConn != null) adbcConn.close()
    if (database != null) database.close()
    if (allocator != null) allocator.close()
  }
}
