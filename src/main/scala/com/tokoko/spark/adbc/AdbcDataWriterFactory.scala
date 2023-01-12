package com.tokoko.spark.adbc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class AdbcDataWriterFactory(schema: StructType, driver: String, url: String, table: String) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new AdbcDataWriter(schema, driver, url, table)
  }
}
