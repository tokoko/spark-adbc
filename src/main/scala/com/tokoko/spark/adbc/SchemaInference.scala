package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcException, AdbcStatusCode}
import org.apache.arrow.vector.types.pojo.Schema

object SchemaInference {
  def run(conn: AdbcConnection, query: String): Schema = {
    try {
      val stmt = conn.createStatement()
      try {
        stmt.setSqlQuery(query)
        stmt.executeSchema()
      } finally stmt.close()
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
        val stmt = conn.createStatement()
        try {
          stmt.setSqlQuery(s"SELECT * FROM ($query) AS t WHERE 1=0")
          val result = stmt.executeQuery()
          try result.getReader.getVectorSchemaRoot.getSchema
          finally result.close()
        } finally stmt.close()
    }
  }
}
