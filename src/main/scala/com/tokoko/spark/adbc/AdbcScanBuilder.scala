package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class AdbcScanBuilder(schema: StructType, driver: String, url: String, query: String) extends ScanBuilder {
  override def build(): Scan = {
    new AdbcScan(driver, schema, url, query)
  }
}
