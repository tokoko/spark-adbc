package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.expressions.NullOrdering

sealed trait IdentifierQuote
object IdentifierQuote {
  case object DoubleQuote extends IdentifierQuote
  case object Backtick extends IdentifierQuote
}

sealed trait LimitOffsetSyntax
object LimitOffsetSyntax {
  case object LimitOffset extends LimitOffsetSyntax
  case object OffsetFetch extends LimitOffsetSyntax
}

sealed trait NullsOrderingSyntax
object NullsOrderingSyntax {
  case object NullsFirstLast extends NullsOrderingSyntax
  case object Unsupported extends NullsOrderingSyntax
}

sealed trait BoolLiteral
object BoolLiteral {
  case object TrueFalse extends BoolLiteral
  case object IntOneZero extends BoolLiteral
}

sealed trait DateTimeLiteral
object DateTimeLiteral {
  case object AnsiKeyword extends DateTimeLiteral
  case object BareString extends DateTimeLiteral
}

case class SqlDialect(
  identifierQuote: IdentifierQuote,
  limitOffsetSyntax: LimitOffsetSyntax,
  nullsOrderingSyntax: NullsOrderingSyntax,
  boolLiteral: BoolLiteral,
  dateTimeLiteral: DateTimeLiteral
)

object SqlDialect {
  val Default: SqlDialect = SqlDialect(
    identifierQuote = IdentifierQuote.DoubleQuote,
    limitOffsetSyntax = LimitOffsetSyntax.LimitOffset,
    nullsOrderingSyntax = NullsOrderingSyntax.NullsFirstLast,
    boolLiteral = BoolLiteral.TrueFalse,
    dateTimeLiteral = DateTimeLiteral.AnsiKeyword
  )

  val Mssql: SqlDialect = SqlDialect(
    identifierQuote = IdentifierQuote.DoubleQuote,
    limitOffsetSyntax = LimitOffsetSyntax.OffsetFetch,
    nullsOrderingSyntax = NullsOrderingSyntax.Unsupported,
    boolLiteral = BoolLiteral.IntOneZero,
    dateTimeLiteral = DateTimeLiteral.BareString
  )

  val Mysql: SqlDialect = SqlDialect(
    identifierQuote = IdentifierQuote.Backtick,
    limitOffsetSyntax = LimitOffsetSyntax.LimitOffset,
    nullsOrderingSyntax = NullsOrderingSyntax.Unsupported,
    boolLiteral = BoolLiteral.TrueFalse,
    dateTimeLiteral = DateTimeLiteral.BareString
  )

  def apply(name: String): SqlDialect = name.toLowerCase match {
    case "mssql" => Mssql
    case "mysql" => Mysql
    case _ => Default
  }

  def fromOptions(dialect: Option[String], jniDriver: Option[String]): SqlDialect =
    apply(dialect.getOrElse(jniDriver.getOrElse("default")))
}

object SqlBuilder {
  def quoteId(dialect: SqlDialect, name: String): String = dialect.identifierQuote match {
    case IdentifierQuote.DoubleQuote => "\"" + name.replace("\"", "\"\"") + "\""
    case IdentifierQuote.Backtick => "`" + name.replace("`", "``") + "`"
  }

  def limitClause(dialect: SqlDialect, limit: Option[Int]): String =
    limit match {
      case None => ""
      case Some(n) => dialect.limitOffsetSyntax match {
        case LimitOffsetSyntax.LimitOffset => s" LIMIT $n"
        case LimitOffsetSyntax.OffsetFetch => s" OFFSET 0 ROWS FETCH NEXT $n ROWS ONLY"
      }
    }

  def requiresOrderByForLimit(dialect: SqlDialect): Boolean =
    dialect.limitOffsetSyntax == LimitOffsetSyntax.OffsetFetch

  def formatSortOrder(dialect: SqlDialect, name: String, dir: String, nullOrdering: NullOrdering): String =
    dialect.nullsOrderingSyntax match {
      case NullsOrderingSyntax.NullsFirstLast =>
        val nulls = nullOrdering match {
          case NullOrdering.NULLS_FIRST => " NULLS FIRST"
          case NullOrdering.NULLS_LAST => " NULLS LAST"
        }
        s"$name $dir$nulls"
      case NullsOrderingSyntax.Unsupported =>
        s"$name $dir"
    }
}
