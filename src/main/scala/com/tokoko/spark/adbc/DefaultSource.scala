package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtilsExtended, CaseInsensitiveStringMap}

import java.util
import collection.JavaConverters._

class DefaultSource extends TableProvider{

  // TODO runs a query to determine schema until ADBC supports more direct method for schema inference
  // TODO ADBC getTableSchema fails on derby table without catalog/schema
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val url = options.get("url")
    val table = options.get("dbtable")
    val query = if (table != null) {
      s"SELECT * FROM $table AS T"
    } else options.get("query")

    val driver = options.get("driver")

    val parameters: Map[String, Object] = Map(AdbcDriver.PARAM_URL -> url)

    Class.forName(driver)

    val database = AdbcDriverManager.getInstance()
      .connect(driver.split('.').init.mkString("."), parameters.asJava)

    val adbcConn = database.connect()

    //val schema = if (table != null && !table.contains(" ")) adbcConn.getTableSchema(null, "APP", table)
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
