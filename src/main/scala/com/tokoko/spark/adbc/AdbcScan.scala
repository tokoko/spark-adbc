package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class AdbcScan(driver: String, schema: StructType, url: String, query: String) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    new AdbcBatch(driver, url, query)
  }
}
