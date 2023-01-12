package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

class AdbcWriteBuilder(schema: StructType, driver: String, url: String, table: String) extends WriteBuilder {
  override def build(): Write = {
    new AdbcWrite(schema, driver, url, table)
  }
}
