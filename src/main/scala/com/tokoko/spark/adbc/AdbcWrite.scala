package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType

class AdbcWrite(schema: StructType, driver: String, params: Map[String, String], table: String) extends Write {

  override def toBatch: BatchWrite = {
    new AdbcBatchWrite(schema, driver, params, table)
  }

}
