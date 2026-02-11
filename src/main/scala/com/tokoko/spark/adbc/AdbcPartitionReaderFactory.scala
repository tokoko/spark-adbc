package com.tokoko.spark.adbc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class AdbcPartitionReaderFactory(driver: String, params: Map[String, String], query: String) extends PartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("This reader only supports columnar reads")
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new AdbcPartitionReader(driver, params, query)
  }
}
