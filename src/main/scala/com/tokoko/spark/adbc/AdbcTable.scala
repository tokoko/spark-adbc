package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._
import java.util

class AdbcTable(schema: StructType) extends Table with SupportsRead with SupportsWrite{
  override def name(): String = ""

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val url = options.get("url")
    val query = options.get("query")
    val driver = options.get("driver")

    new AdbcScanBuilder(schema, driver, url, query)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val url = info.options().get("url")
    val table = info.options().get("dbtable")
    val driver = info.options().get("driver")

    new AdbcWriteBuilder(schema, driver, url, table)
  }
}
