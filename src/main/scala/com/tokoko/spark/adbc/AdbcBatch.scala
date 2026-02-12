package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

class AdbcBatch(driver: String, params: Map[String, String], queries: Array[String]) extends Batch {
  override def planInputPartitions(): Array[InputPartition] = {
    queries.map(q => new AdbcPartition(q): InputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new AdbcPartitionReaderFactory(driver, params)
  }
}
