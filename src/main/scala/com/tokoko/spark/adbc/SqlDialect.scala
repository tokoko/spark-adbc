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

/** How `%` and `_` are escaped in a LIKE pattern. */
sealed trait LikeEscapeSyntax
object LikeEscapeSyntax {
  /** `LIKE '...' ESCAPE '!'` with a chosen escape character. */
  case object EscapeClause extends LikeEscapeSyntax
  /** Backslash escapes and there is no usable ESCAPE clause. */
  case object ImplicitBackslash extends LikeEscapeSyntax
}

/** What a backslash means inside a quoted string literal. */
sealed trait StringBackslash
object StringBackslash {
  case object Literal extends StringBackslash
  case object Escape extends StringBackslash
}

/** How a string literal holding non-ASCII characters has to be written. */
sealed trait NonAsciiLiteral
object NonAsciiLiteral {
  case object Plain extends NonAsciiLiteral
  /** `N'...'`; a plain literal is converted through a single-byte code page first. */
  case object NationalPrefix extends NonAsciiLiteral
}

/** Literal for a point in time (timestamp with time zone), written in UTC with an explicit offset. */
sealed trait InstantLiteral
object InstantLiteral {
  /** `TIMESTAMP '2024-01-01 00:00:00.000000+00:00'` */
  case object KeywordWithOffset extends InstantLiteral
  /** `TIMESTAMP WITH TIME ZONE '2024-01-01 00:00:00.000000+00:00'` */
  case object WithTimeZoneKeyword extends InstantLiteral
  /** `'2024-01-01 00:00:00.000000+00:00'` */
  case object BareStringWithOffset extends InstantLiteral
  /** No literal carries an offset; comparisons against instants are not pushed down. */
  case object Unsupported extends InstantLiteral
}

case class SqlDialect(
  identifierQuote: IdentifierQuote,
  limitOffsetSyntax: LimitOffsetSyntax,
  nullsOrderingSyntax: NullsOrderingSyntax,
  boolLiteral: BoolLiteral,
  dateTimeLiteral: DateTimeLiteral,
  likeEscapeSyntax: LikeEscapeSyntax,
  stringBackslash: StringBackslash,
  nonAsciiLiteral: NonAsciiLiteral,
  instantLiteral: InstantLiteral
)

object SqlDialect {
  val Default: SqlDialect = SqlDialect(
    identifierQuote = IdentifierQuote.DoubleQuote,
    limitOffsetSyntax = LimitOffsetSyntax.LimitOffset,
    nullsOrderingSyntax = NullsOrderingSyntax.NullsFirstLast,
    boolLiteral = BoolLiteral.TrueFalse,
    dateTimeLiteral = DateTimeLiteral.AnsiKeyword,
    likeEscapeSyntax = LikeEscapeSyntax.EscapeClause,
    stringBackslash = StringBackslash.Literal,
    nonAsciiLiteral = NonAsciiLiteral.Plain,
    // PostgreSQL and DuckDB silently drop the offset of TIMESTAMP '...+00:00', so the default is
    // the spelling that an engine either honours or rejects.
    instantLiteral = InstantLiteral.WithTimeZoneKeyword
  )

  val Mssql: SqlDialect = Default.copy(
    limitOffsetSyntax = LimitOffsetSyntax.OffsetFetch,
    nullsOrderingSyntax = NullsOrderingSyntax.Unsupported,
    boolLiteral = BoolLiteral.IntOneZero,
    dateTimeLiteral = DateTimeLiteral.BareString,
    nonAsciiLiteral = NonAsciiLiteral.NationalPrefix,
    instantLiteral = InstantLiteral.BareStringWithOffset
  )

  val Mysql: SqlDialect = Default.copy(
    identifierQuote = IdentifierQuote.Backtick,
    nullsOrderingSyntax = NullsOrderingSyntax.Unsupported,
    dateTimeLiteral = DateTimeLiteral.BareString,
    stringBackslash = StringBackslash.Escape,
    instantLiteral = InstantLiteral.BareStringWithOffset
  )

  // ClickHouse parses TIMESTAMP '...' as a second-precision DateTime, so fractional
  // seconds only survive as a bare string, and it accepts no literal with an offset.
  val Clickhouse: SqlDialect = Default.copy(
    dateTimeLiteral = DateTimeLiteral.BareString,
    likeEscapeSyntax = LikeEscapeSyntax.ImplicitBackslash,
    stringBackslash = StringBackslash.Escape,
    instantLiteral = InstantLiteral.Unsupported
  )

  // Trino spells every timestamp literal TIMESTAMP '...' and types it by its content.
  val Trino: SqlDialect = Default.copy(instantLiteral = InstantLiteral.KeywordWithOffset)

  // Spark SQL reads "..." as a string and treats backslash as an escape, like MySQL, but has
  // NULLS FIRST/LAST and typed date/time literals.
  val Spark: SqlDialect = Default.copy(
    identifierQuote = IdentifierQuote.Backtick,
    stringBackslash = StringBackslash.Escape,
    instantLiteral = InstantLiteral.KeywordWithOffset
  )

  val Datafusion: SqlDialect = Default.copy(likeEscapeSyntax = LikeEscapeSyntax.ImplicitBackslash)

  def apply(name: String): SqlDialect = name.toLowerCase match {
    case "mssql" => Mssql
    case "mysql" => Mysql
    case "clickhouse" | "chdb" => Clickhouse
    case "trino" => Trino
    case "spark" => Spark
    case "datafusion" => Datafusion
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
