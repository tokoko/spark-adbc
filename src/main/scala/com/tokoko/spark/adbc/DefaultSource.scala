package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtilsExtended, CaseInsensitiveStringMap}

import java.util
import collection.JavaConverters._

class DefaultSource extends TableProvider{

  private val reservedKeys = Set("driver", "dbtable", "query")

  // TODO runs a query to determine schema until ADBC supports more direct method for schema inference
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val table = options.get("dbtable")
    val query = if (table != null) {
      s"SELECT * FROM $table AS T"
    } else options.get("query")

    val driver = options.get("driver")

    val parameters: java.util.Map[String, Object] = options.asScala
      .filterKeys(k => !reservedKeys.contains(k.toLowerCase))
      .mapValues(v => v: Object).asJava

    val allocator = new RootAllocator(Long.MaxValue)
    val database = AdbcDriverManager.getInstance()
      .connect(driver, allocator, parameters)

    val adbcConn = database.connect()

    val statement = adbcConn.createStatement()
    statement.setSqlQuery(query)
    val schema = statement.executeQuery()
      .getReader.getVectorSchemaRoot.getSchema

    ArrowUtilsExtended.fromArrowSchema(schema)
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new AdbcTable(schema)
  }
}
