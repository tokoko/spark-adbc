package com.tokoko.spark.adbc

import org.apache.spark.sql.sources._

object FilterConverter {

  def canConvert(filter: Filter): Boolean = filter match {
    case EqualTo(_, _) => true
    case GreaterThan(_, _) => true
    case GreaterThanOrEqual(_, _) => true
    case LessThan(_, _) => true
    case LessThanOrEqual(_, _) => true
    case IsNull(_) => true
    case IsNotNull(_) => true
    case In(_, _) => true
    case And(left, right) => canConvert(left) && canConvert(right)
    case Or(left, right) => canConvert(left) && canConvert(right)
    case Not(child) => canConvert(child)
    case StringStartsWith(_, _) => true
    case StringEndsWith(_, _) => true
    case StringContains(_, _) => true
    case _ => false
  }

  def convert(filter: Filter): String = filter match {
    case EqualTo(attr, value) => s"$attr = ${toLiteral(value)}"
    case GreaterThan(attr, value) => s"$attr > ${toLiteral(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${toLiteral(value)}"
    case LessThan(attr, value) => s"$attr < ${toLiteral(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${toLiteral(value)}"
    case IsNull(attr) => s"$attr IS NULL"
    case IsNotNull(attr) => s"$attr IS NOT NULL"
    case In(attr, values) => s"$attr IN (${values.map(toLiteral).mkString(", ")})"
    case And(left, right) => s"(${convert(left)}) AND (${convert(right)})"
    case Or(left, right) => s"(${convert(left)}) OR (${convert(right)})"
    case Not(child) => s"NOT (${convert(child)})"
    case StringStartsWith(attr, value) => s"$attr LIKE '${escapeLike(value)}%'"
    case StringEndsWith(attr, value) => s"$attr LIKE '%${escapeLike(value)}'"
    case StringContains(attr, value) => s"$attr LIKE '%${escapeLike(value)}%'"
  }

  private def toLiteral(value: Any): String = value match {
    case s: String => s"'${s.replace("'", "''")}'"
    case null => "NULL"
    case v => v.toString
  }

  private def escapeLike(value: String): String = {
    value.replace("'", "''").replace("%", "\\%").replace("_", "\\_")
  }

}
