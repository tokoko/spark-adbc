package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtilsExtended, CaseInsensitiveStringMap}

import java.util
import scala.jdk.CollectionConverters._

class DefaultSource extends TableProvider{

  private val reservedKeys = Set("driver", "dbtable", "query", "dialect",
    "partitioncolumn", "lowerbound", "upperbound", "numpartitions")

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val baseRelation = Option(options.get("dbtable")).getOrElse {
      s"(${options.get("query")}) AS T"
    }
    val driver = options.get("driver")

    val parameters: java.util.Map[String, Object] = options.asScala
      .view.filterKeys(k => !reservedKeys.contains(k.toLowerCase))
      .mapValues(v => v: Object).toMap.asJava

    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val database = AdbcDriverManager.getInstance().connect(driver, allocator, parameters)
      try {
        val adbcConn = database.connect()
        try ArrowUtilsExtended.fromArrowSchema(SchemaInference.run(adbcConn, s"SELECT * FROM $baseRelation"))
        finally adbcConn.close()
      } finally database.close()
    } finally allocator.close()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new AdbcTable(schema)
  }
}
