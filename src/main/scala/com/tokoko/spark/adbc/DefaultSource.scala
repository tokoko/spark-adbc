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

  private val reservedKeys = Set("driver", "dbtable", "query", "dialect")

  // TODO runs a query to determine schema until ADBC supports more direct method for schema inference
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val dialect = SqlDialect.fromOptions(Option(options.get("dialect")), Option(options.get("jni.driver")))

    val baseRelation = Option(options.get("dbtable")).getOrElse {
      s"(${options.get("query")}) AS T"
    }
    val query = dialect.schemaInferenceQuery("*", baseRelation, "")

    val driver = options.get("driver")

    val parameters: java.util.Map[String, Object] = options.asScala
      .view.filterKeys(k => !reservedKeys.contains(k.toLowerCase))
      .mapValues(v => v: Object).toMap.asJava

    val allocator = new RootAllocator(Long.MaxValue)
    val database = AdbcDriverManager.getInstance()
      .connect(driver, allocator, parameters)
    try {
      val adbcConn = database.connect()
      try {
        val statement = adbcConn.createStatement()
        try {
          statement.setSqlQuery(query)
          val result = statement.executeQuery()
          try {
            val schema = result.getReader.getVectorSchemaRoot.getSchema
            ArrowUtilsExtended.fromArrowSchema(schema)
          } finally result.close()
        } finally statement.close()
      } finally adbcConn.close()
    } finally database.close()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new AdbcTable(schema)
  }
}
