package com.tokoko.spark.adbc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class AdbcPartitionReaderFactory(driver: String, params: Map[String, String], query: String) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new AdbcPartitionReader(driver, params, query)
  }
}
