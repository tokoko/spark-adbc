package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

class AdbcBatch(driver: String, url: String, query: String) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = Array(
    new AdbcPartition
  )

  override def createReaderFactory(): PartitionReaderFactory = {
    new AdbcPartitionReaderFactory(driver, url, query)
  }
}
