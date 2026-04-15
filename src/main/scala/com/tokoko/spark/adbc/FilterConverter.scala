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

  def convert(filter: Filter, dialect: SqlDialect): String = {
    def q(attr: String): String = SqlBuilder.quoteId(dialect, attr)
    def lit(v: Any): String = toLiteral(v, dialect)
    filter match {
      case EqualTo(attr, value) => s"${q(attr)} = ${lit(value)}"
      case GreaterThan(attr, value) => s"${q(attr)} > ${lit(value)}"
      case GreaterThanOrEqual(attr, value) => s"${q(attr)} >= ${lit(value)}"
      case LessThan(attr, value) => s"${q(attr)} < ${lit(value)}"
      case LessThanOrEqual(attr, value) => s"${q(attr)} <= ${lit(value)}"
      case IsNull(attr) => s"${q(attr)} IS NULL"
      case IsNotNull(attr) => s"${q(attr)} IS NOT NULL"
      case In(attr, values) => s"${q(attr)} IN (${values.map(lit).mkString(", ")})"
      case And(left, right) => s"(${convert(left, dialect)}) AND (${convert(right, dialect)})"
      case Or(left, right) => s"(${convert(left, dialect)}) OR (${convert(right, dialect)})"
      case Not(child) => s"NOT (${convert(child, dialect)})"
      case StringStartsWith(attr, value) => s"${q(attr)} LIKE ${likePattern("", value, "%")}"
      case StringEndsWith(attr, value) => s"${q(attr)} LIKE ${likePattern("%", value, "")}"
      case StringContains(attr, value) => s"${q(attr)} LIKE ${likePattern("%", value, "%")}"
    }
  }

  private def toLiteral(value: Any, dialect: SqlDialect): String = value match {
    case null => "NULL"
    case s: String => s"'${s.replace("'", "''")}'"
    case b: Boolean => dialect.boolLiteral match {
      case BoolLiteral.TrueFalse => if (b) "TRUE" else "FALSE"
      case BoolLiteral.IntOneZero => if (b) "1" else "0"
    }
    case d: java.sql.Date => dateLiteral(d.toString, dialect)
    case d: java.time.LocalDate => dateLiteral(d.toString, dialect)
    case t: java.sql.Timestamp => timestampLiteral(t.toString, dialect)
    case t: java.time.Instant => timestampLiteral(t.toString.replace("T", " ").replace("Z", ""), dialect)
    case v => v.toString
  }

  private def dateLiteral(s: String, dialect: SqlDialect): String =
    dialect.dateTimeLiteral match {
      case DateTimeLiteral.AnsiKeyword => s"DATE '$s'"
      case DateTimeLiteral.BareString => s"'$s'"
    }

  private def timestampLiteral(s: String, dialect: SqlDialect): String =
    dialect.dateTimeLiteral match {
      case DateTimeLiteral.AnsiKeyword => s"TIMESTAMP '$s'"
      case DateTimeLiteral.BareString => s"'$s'"
    }

  private def likePattern(prefix: String, value: String, suffix: String): String = {
    val escaped = value
      .replace("!", "!!")
      .replace("%", "!%")
      .replace("_", "!_")
      .replace("'", "''")
    s"'$prefix$escaped$suffix' ESCAPE '!'"
  }

}
