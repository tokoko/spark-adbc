package com.tokoko.spark.adbc

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources._

object FilterConverter {

  def canConvert(filter: Filter, dialect: SqlDialect): Boolean = filter match {
    case EqualTo(_, value) => hasLiteral(value, dialect)
    case GreaterThan(_, value) => hasLiteral(value, dialect)
    case GreaterThanOrEqual(_, value) => hasLiteral(value, dialect)
    case LessThan(_, value) => hasLiteral(value, dialect)
    case LessThanOrEqual(_, value) => hasLiteral(value, dialect)
    case IsNull(_) => true
    case IsNotNull(_) => true
    case In(_, values) => values.forall(hasLiteral(_, dialect))
    case And(left, right) => canConvert(left, dialect) && canConvert(right, dialect)
    case Or(left, right) => canConvert(left, dialect) && canConvert(right, dialect)
    case Not(child) => canConvert(child, dialect)
    case StringStartsWith(_, _) => true
    case StringEndsWith(_, _) => true
    case StringContains(_, _) => true
    case _ => false
  }

  private def hasLiteral(value: Any, dialect: SqlDialect): Boolean = value match {
    // Binary literals have no portable spelling, so comparisons against them stay in Spark.
    case _: Array[_] => false
    case _: java.sql.Timestamp | _: java.time.Instant => dialect.instantLiteral != InstantLiteral.Unsupported
    case _ => true
  }

  def convert(filter: Filter, dialect: SqlDialect): String = {
    // Spark hands over names that aren't plain identifiers already backtick-quoted.
    def q(attr: String): String =
      CatalystSqlParser.parseMultipartIdentifier(attr).map(SqlBuilder.quoteId(dialect, _)).mkString(".")
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
      case StringStartsWith(attr, value) => s"${q(attr)} LIKE ${likePattern("", value, "%", dialect)}"
      case StringEndsWith(attr, value) => s"${q(attr)} LIKE ${likePattern("%", value, "", dialect)}"
      case StringContains(attr, value) => s"${q(attr)} LIKE ${likePattern("%", value, "%", dialect)}"
    }
  }

  private def stringLiteral(s: String, dialect: SqlDialect): String = {
    val backslashed = dialect.stringBackslash match {
      case StringBackslash.Literal => s
      case StringBackslash.Escape => s.replace("\\", "\\\\")
    }
    val prefix = dialect.nonAsciiLiteral match {
      case NonAsciiLiteral.NationalPrefix if s.exists(_ > 127) => "N"
      case _ => ""
    }
    s"$prefix'${backslashed.replace("'", "''")}'"
  }

  private def toLiteral(value: Any, dialect: SqlDialect): String = value match {
    case null => "NULL"
    case s: String => stringLiteral(s, dialect)
    case b: Boolean => dialect.boolLiteral match {
      case BoolLiteral.TrueFalse => if (b) "TRUE" else "FALSE"
      case BoolLiteral.IntOneZero => if (b) "1" else "0"
    }
    case d: java.sql.Date => dateLiteral(d.toString, dialect)
    case d: java.time.LocalDate => dateLiteral(d.toString, dialect)
    case t: java.sql.Timestamp => instantLiteral(t.toInstant, dialect)
    case t: java.time.LocalDateTime => timestampLiteral(t.format(localDateTimeFormat), dialect)
    case t: java.time.Instant => instantLiteral(t, dialect)
    case v => v.toString
  }

  private val localDateTimeFormat = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  // Always UTC with an explicit offset: neither the JVM zone nor the session zone can shift it.
  private def instantLiteral(t: java.time.Instant, dialect: SqlDialect): String = {
    val s = java.time.LocalDateTime.ofInstant(t, java.time.ZoneOffset.UTC).format(localDateTimeFormat) + "+00:00"
    dialect.instantLiteral match {
      case InstantLiteral.KeywordWithOffset => s"TIMESTAMP '$s'"
      case InstantLiteral.WithTimeZoneKeyword => s"TIMESTAMP WITH TIME ZONE '$s'"
      case InstantLiteral.BareStringWithOffset => s"'$s'"
      case InstantLiteral.Unsupported =>
        throw new IllegalStateException("instant literals are not supported by this dialect")
    }
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

  private def likePattern(prefix: String, value: String, suffix: String, dialect: SqlDialect): String =
    dialect.likeEscapeSyntax match {
      case LikeEscapeSyntax.EscapeClause =>
        val escaped = value.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        s"${stringLiteral(prefix + escaped + suffix, dialect)} ESCAPE '!'"
      case LikeEscapeSyntax.ImplicitBackslash =>
        val escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        stringLiteral(prefix + escaped + suffix, dialect)
    }

}
