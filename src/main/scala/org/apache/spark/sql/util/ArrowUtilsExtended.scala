package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types.StructType

object ArrowUtilsExtended {

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(schema)
  }

}
