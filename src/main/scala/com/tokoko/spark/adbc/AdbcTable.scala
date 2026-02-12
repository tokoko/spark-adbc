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

  private val reservedKeys = Set("driver", "dbtable", "query", "dialect",
    "partitioncolumn", "lowerbound", "upperbound", "numpartitions")

  private def extractParams(options: java.util.Map[String, String]): Map[String, String] = {
    options.asScala.filterKeys(k => !reservedKeys.contains(k.toLowerCase)).toMap
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val dbtable = Option(options.get("dbtable"))
    val query = Option(options.get("query"))
    val driver = options.get("driver")
    val dialect = SqlDialect.fromOptions(Option(options.get("dialect")), Option(options.get("jni.driver")))
    val params = extractParams(options)

    val partitionColumn = Option(options.get("partitionColumn"))
    val partLowerBound = Option(options.get("lowerBound")).map(_.toLong)
    val partUpperBound = Option(options.get("upperBound")).map(_.toLong)
    val partNumPartitions = Option(options.get("numPartitions")).map(_.toInt)

    val partitionOpts = Seq(partitionColumn, partLowerBound, partUpperBound, partNumPartitions)
    val provided = partitionOpts.count(_.isDefined)
    if (provided > 0 && provided < 4) {
      throw new IllegalArgumentException(
        "All partitioning options (partitionColumn, lowerBound, upperBound, numPartitions) " +
        "must be specified together, or none of them."
      )
    }
    if (partNumPartitions.exists(_ <= 0)) {
      throw new IllegalArgumentException("numPartitions must be a positive integer")
    }
    if (partLowerBound.isDefined && partUpperBound.isDefined && partLowerBound.get >= partUpperBound.get) {
      throw new IllegalArgumentException(
        s"lowerBound (${partLowerBound.get}) must be strictly less than upperBound (${partUpperBound.get})"
      )
    }

    new AdbcScanBuilder(schema, driver, params, dbtable, query, dialect,
      partitionColumn, partLowerBound, partUpperBound, partNumPartitions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val table = info.options().get("dbtable")
    val driver = info.options().get("driver")
    val params = extractParams(info.options())

    new AdbcWriteBuilder(schema, driver, params, table)
  }
}
