package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._
import java.util

class AdbcTable(schema: StructType) extends Table with SupportsRead with SupportsWrite{
  override def name(): String = ""

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE).asJava

  private val reservedKeys = Set("driver", "dbtable", "query")

  private def extractParams(options: java.util.Map[String, String]): Map[String, String] = {
    options.asScala.filterKeys(k => !reservedKeys.contains(k.toLowerCase)).toMap
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val dbtable = Option(options.get("dbtable"))
    val query = Option(options.get("query"))
    val driver = options.get("driver")
    val params = extractParams(options)

    new AdbcScanBuilder(schema, driver, params, dbtable, query)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val table = info.options().get("dbtable")
    val driver = info.options().get("driver")
    val params = extractParams(info.options())

    new AdbcWriteBuilder(schema, driver, params, table)
  }
}
