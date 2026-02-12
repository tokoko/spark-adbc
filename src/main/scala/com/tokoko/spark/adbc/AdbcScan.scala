package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class AdbcScan(driver: String, schema: StructType, params: Map[String, String], queries: Array[String]) extends Scan {
  override def readSchema(): StructType = schema

  override def columnarSupportMode(): Scan.ColumnarSupportMode = Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = {
    new AdbcBatch(driver, params, queries)
  }
}
