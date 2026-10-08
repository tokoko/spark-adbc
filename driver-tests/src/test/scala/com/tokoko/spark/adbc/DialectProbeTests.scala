package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.AdbcConnection

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Sends each syntax variant a query generator has to choose between straight to the engine
 * and records which ones it accepts. The result is the per-engine matrix in
 * `target/dialect-matrix.md`: evidence for which dialect flags an engine would have to
 * report, and which differences no flag describes.
 */
trait DialectProbeTests { this: AdbcSuiteBase =>

  private sealed trait Expect
  /** First column of the result, in order. */
  private case class Rows(values: Seq[String]) extends Expect
  private case class Count(n: Int) extends Expect

  private case class Probe(group: String, name: String, requires: Seq[(String, String)],
      sql: (String => String) => String, expect: Expect)

  private val LimitGroup = "Row limit and offset (577 SQL_SUPPORTED_LIMIT_OFFSET)"
  private val NullsGroup = "Explicit null ordering (578 SQL_SUPPORTED_NULLS_ORDERING)"
  private val BoolGroup = "Boolean literals (579 SQL_SUPPORTED_BOOLEAN_LITERAL)"
  private val DateTimeGroup = "Date and time literals (580 SQL_SUPPORTED_DATETIME_LITERAL)"
  private val QuoteGroup = "Identifier quoting (504 SQL_IDENTIFIER_QUOTE_CHAR)"
  private val StringGroup = "String literals (no flag in the PR)"
  private val LikeGroup = "LIKE escaping (523 SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE covers the clause only)"
  private val DerivedGroup = "Derived tables (no flag)"
  private val AggGroup = "Aggregate semantics (no flag)"

  private def ids(xs: Int*): Rows = Rows(xs.map(_.toString))

  private val probes: Seq[Probe] = {
    val b = ArrayBuffer[Probe]()
    def add(group: String, name: String, expect: Expect, requires: (String, String)*)(sql: (String => String) => String): Unit =
      b += Probe(group, name, requires, sql, expect)

    // ---- 577 ----
    add(LimitGroup, "LIMIT n", ids(1, 2))(t => s"SELECT id FROM ${t("sortable")} ORDER BY id LIMIT 2")
    add(LimitGroup, "LIMIT n OFFSET m", ids(2, 3))(t => s"SELECT id FROM ${t("sortable")} ORDER BY id LIMIT 2 OFFSET 1")
    add(LimitGroup, "OFFSET m LIMIT n", ids(2, 3))(t => s"SELECT id FROM ${t("sortable")} ORDER BY id OFFSET 1 LIMIT 2")
    add(LimitGroup, "LIMIT m, n", ids(2, 3))(t => s"SELECT id FROM ${t("sortable")} ORDER BY id LIMIT 1, 2")
    add(LimitGroup, "OFFSET m ROWS FETCH NEXT n ROWS ONLY", ids(2, 3))(t =>
      s"SELECT id FROM ${t("sortable")} ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
    add(LimitGroup, "FETCH FIRST n ROWS ONLY, no OFFSET", ids(1, 2))(t =>
      s"SELECT id FROM ${t("sortable")} ORDER BY id FETCH FIRST 2 ROWS ONLY")
    add(LimitGroup, "OFFSET m ROWS, no FETCH", ids(4, 5))(t => s"SELECT id FROM ${t("sortable")} ORDER BY id OFFSET 3 ROWS")
    add(LimitGroup, "SELECT TOP n", ids(1, 2))(t => s"SELECT TOP 2 id FROM ${t("sortable")} ORDER BY id")
    add(LimitGroup, "LIMIT n without ORDER BY", Count(2))(t => s"SELECT id FROM ${t("sortable")} LIMIT 2")
    add(LimitGroup, "OFFSET/FETCH without ORDER BY", Count(2))(t =>
      s"SELECT id FROM ${t("sortable")} OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY")
    add(LimitGroup, "OFFSET/FETCH after ORDER BY (SELECT NULL)", Count(2))(t =>
      s"SELECT id FROM ${t("sortable")} ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY")
    add(LimitGroup, "LIMIT n inside a derived table", ids(2))(t =>
      s"SELECT COUNT(*) FROM (SELECT id FROM ${t("sortable")} LIMIT 2) AS x")

    // ---- 578 ----
    add(NullsGroup, "ASC NULLS FIRST", ids(2, 5, 1, 4, 3))(t => s"SELECT id FROM ${t("sortable")} ORDER BY sort_key ASC NULLS FIRST, id")
    add(NullsGroup, "ASC NULLS LAST", ids(1, 4, 3, 2, 5))(t => s"SELECT id FROM ${t("sortable")} ORDER BY sort_key ASC NULLS LAST, id")
    add(NullsGroup, "DESC NULLS FIRST", ids(2, 5, 3, 4, 1))(t => s"SELECT id FROM ${t("sortable")} ORDER BY sort_key DESC NULLS FIRST, id")
    add(NullsGroup, "DESC NULLS LAST", ids(3, 4, 1, 2, 5))(t => s"SELECT id FROM ${t("sortable")} ORDER BY sort_key DESC NULLS LAST, id")
    add(NullsGroup, "CASE WHEN x IS NULL emulation", ids(1, 4, 3, 2, 5))(t =>
      s"SELECT id FROM ${t("sortable")} ORDER BY CASE WHEN sort_key IS NULL THEN 1 ELSE 0 END, sort_key, id")

    // ---- 579 ----
    val active = "events" -> "active"
    add(BoolGroup, "col = TRUE", ids(1, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active = TRUE ORDER BY id")
    add(BoolGroup, "col = FALSE", ids(2), active)(t => s"SELECT id FROM ${t("events")} WHERE active = FALSE ORDER BY id")
    add(BoolGroup, "col = 1", ids(1, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active = 1 ORDER BY id")
    add(BoolGroup, "col = 0", ids(2), active)(t => s"SELECT id FROM ${t("events")} WHERE active = 0 ORDER BY id")
    add(BoolGroup, "col = 'true'", ids(1, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active = 'true' ORDER BY id")
    add(BoolGroup, "bare column as predicate", ids(1, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active ORDER BY id")
    add(BoolGroup, "NOT column", ids(2), active)(t => s"SELECT id FROM ${t("events")} WHERE NOT active ORDER BY id")
    add(BoolGroup, "col IS TRUE", ids(1, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active IS TRUE ORDER BY id")
    add(BoolGroup, "col IN (TRUE, FALSE)", ids(1, 2, 3), active)(t => s"SELECT id FROM ${t("events")} WHERE active IN (TRUE, FALSE) ORDER BY id")
    add(BoolGroup, "WHERE TRUE", ids(1, 2, 3))(t => s"SELECT id FROM ${t("events")} WHERE TRUE ORDER BY id")

    // ---- 580 ----
    val eventDate = "events" -> "event_date"
    add(DateTimeGroup, "date: DATE '...'", ids(2, 3), eventDate)(t =>
      s"SELECT id FROM ${t("events")} WHERE event_date > DATE '2024-03-01' ORDER BY id")
    add(DateTimeGroup, "date: bare '...'", ids(2, 3), eventDate)(t =>
      s"SELECT id FROM ${t("events")} WHERE event_date > '2024-03-01' ORDER BY id")
    add(DateTimeGroup, "date: {d '...'} escape", ids(2, 3), eventDate)(t =>
      s"SELECT id FROM ${t("events")} WHERE event_date > {d '2024-03-01'} ORDER BY id")
    add(DateTimeGroup, "date: CAST('...' AS DATE)", ids(2, 3), eventDate)(t =>
      s"SELECT id FROM ${t("events")} WHERE event_date > CAST('2024-03-01' AS DATE) ORDER BY id")

    val cTs = "all_types" -> "c_ts"
    add(DateTimeGroup, "timestamp: TIMESTAMP '...', whole seconds", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > TIMESTAMP '2024-01-15 10:29:59' ORDER BY id")
    add(DateTimeGroup, "timestamp: TIMESTAMP '...', fractional seconds", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > TIMESTAMP '2024-01-15 10:29:59.500000' ORDER BY id")
    add(DateTimeGroup, "timestamp: bare '...', fractional seconds", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > '2024-01-15 10:29:59.500000' ORDER BY id")
    add(DateTimeGroup, "timestamp: bare ISO 8601 with T", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > '2024-01-15T10:29:59.500000' ORDER BY id")
    add(DateTimeGroup, "timestamp: {ts '...'} escape", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > {ts '2024-01-15 10:29:59.500000'} ORDER BY id")
    add(DateTimeGroup, "timestamp: CAST('...' AS TIMESTAMP)", ids(1, 4), cTs)(t =>
      s"SELECT id FROM ${t("all_types")} WHERE c_ts > CAST('2024-01-15 10:29:59.500000' AS TIMESTAMP) ORDER BY id")

    // Row 2 is 23:59:59.999999Z, written here at +05:30: an offset that is neither UTC nor the
    // test JVM's zone, compared by equality, so an engine that drops the offset or reinterprets
    // the value in its session zone cannot match by accident.
    val tsTz = "tz_events" -> "ts_tz"
    add(DateTimeGroup, "timestamptz: TIMESTAMP '... +05:30'", ids(2), tsTz)(t =>
      s"SELECT id FROM ${t("tz_events")} WHERE ts_tz = TIMESTAMP '2024-06-21 05:29:59.999999+05:30' ORDER BY id")
    add(DateTimeGroup, "timestamptz: TIMESTAMP WITH TIME ZONE '... +05:30'", ids(2), tsTz)(t =>
      s"SELECT id FROM ${t("tz_events")} WHERE ts_tz = TIMESTAMP WITH TIME ZONE '2024-06-21 05:29:59.999999+05:30' ORDER BY id")
    add(DateTimeGroup, "timestamptz: bare '... +05:30'", ids(2), tsTz)(t =>
      s"SELECT id FROM ${t("tz_events")} WHERE ts_tz = '2024-06-21 05:29:59.999999+05:30' ORDER BY id")
    add(DateTimeGroup, "timestamptz: bare ISO 8601 with T and offset", ids(2), tsTz)(t =>
      s"SELECT id FROM ${t("tz_events")} WHERE ts_tz = '2024-06-21T05:29:59.999999+05:30' ORDER BY id")
    add(DateTimeGroup, "timestamptz: bare '...' without offset is UTC", ids(2), tsTz)(t =>
      s"SELECT id FROM ${t("tz_events")} WHERE ts_tz = '2024-06-20 23:59:59.999999' ORDER BY id")

    // ---- 504 ----
    add(QuoteGroup, "double quotes", ids(2, 3))(t => s"""SELECT id FROM ${t("reserved_kw")} WHERE "order" > 15 ORDER BY id""")
    add(QuoteGroup, "backticks", ids(2, 3))(t => s"SELECT id FROM ${t("reserved_kw")} WHERE `order` > 15 ORDER BY id")
    add(QuoteGroup, "square brackets", ids(2, 3))(t => s"SELECT id FROM ${t("reserved_kw")} WHERE [order] > 15 ORDER BY id")

    // ---- not covered by any flag ----
    add(StringGroup, "'' is one quote", ids(1))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'O''Brien'")
    add(StringGroup, "backslash is an ordinary character", ids(2))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'C:\\temp\\new'")
    add(StringGroup, "backslash is an escape (\\\\ is one backslash)", ids(2))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'C:\\\\temp\\\\new'")
    add(StringGroup, "non-ASCII literal", ids(9))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'ქართული'")
    add(StringGroup, "N'...' national literal", ids(9))(t => s"SELECT id FROM ${t("strings")} WHERE s = N'ქართული'")
    add(StringGroup, "equality is case sensitive", ids(7))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'robin' ORDER BY id")
    add(StringGroup, "trailing spaces are significant", ids(14))(t => s"SELECT id FROM ${t("strings")} WHERE s = 'trail' ORDER BY id")

    add(LikeGroup, "LIKE ... ESCAPE '!'", ids(4))(t => s"SELECT id FROM ${t("strings")} WHERE s LIKE '%!_%' ESCAPE '!' ORDER BY id")
    add(LikeGroup, "LIKE ... ESCAPE '\\'", ids(4))(t => s"SELECT id FROM ${t("strings")} WHERE s LIKE '%\\_%' ESCAPE '\\' ORDER BY id")
    add(LikeGroup, "backslash escapes with no ESCAPE clause", ids(4))(t => s"SELECT id FROM ${t("strings")} WHERE s LIKE '%\\_%' ORDER BY id")
    add(LikeGroup, "LIKE is case sensitive", ids(7))(t => s"SELECT id FROM ${t("strings")} WHERE s LIKE 'rob%' ORDER BY id")

    add(DerivedGroup, "(subquery) AS alias", ids(5))(t => s"SELECT COUNT(*) FROM (SELECT id FROM ${t("sortable")}) AS T")
    add(DerivedGroup, "(subquery) alias", ids(5))(t => s"SELECT COUNT(*) FROM (SELECT id FROM ${t("sortable")}) T")
    add(DerivedGroup, "(subquery) without alias", ids(5))(t => s"SELECT COUNT(*) FROM (SELECT id FROM ${t("sortable")})")
    add(DerivedGroup, "WHERE 1=0 around a derived table", Count(0))(t =>
      s"SELECT * FROM (SELECT id FROM ${t("sortable")}) AS T WHERE 1=0")

    add(AggGroup, "COUNT(DISTINCT x)", ids(3))(t => s"SELECT COUNT(DISTINCT sort_key) FROM ${t("sortable")}")
    add(AggGroup, "AVG(int) keeps the fraction", Rows(Seq("1.5")))(t => s"SELECT AVG(id) FROM ${t("sortable")} WHERE id <= 2")
    add(AggGroup, "SUM(int) widens past 32 bits", Rows(Seq("2147483747")), "all_types" -> "c_int")(t =>
      s"SELECT SUM(c_int) FROM ${t("all_types")} WHERE id IN (1, 4)")
    b.toSeq
  }

  private def firstColumn(conn: AdbcConnection, sql: String): Seq[String] = {
    val stmt = conn.createStatement()
    try {
      stmt.setSqlQuery(sql)
      val result = stmt.executeQuery()
      try {
        val reader = result.getReader
        val out = ArrayBuffer[String]()
        while (reader.loadNextBatch()) {
          val v = reader.getVectorSchemaRoot.getVector(0)
          for (i <- 0 until v.getValueCount) out += (v.getObject(i) match {
            case null => "NULL"
            case n: Number => new java.math.BigDecimal(n.toString).stripTrailingZeros.toPlainString
            case other => other.toString
          })
        }
        out.toSeq
      } finally result.close()
    } finally stmt.close()
  }

  /** (group, name) -> (outcome, detail); every probe runs on its own connection. */
  private lazy val outcomes: Map[(String, String), (String, String)] = probes.map { p =>
    val sql = p.sql(tableRef)
    val (outcome, detail) =
      if (!p.requires.forall { case (table, column) => hasColumn(table, column) }) (DialectReport.NotApplicable, "")
      else try {
        val got = withConnection(firstColumn(_, sql))
        val matches = p.expect match {
          case Rows(values) => got == values
          case Count(n) => got.size == n
        }
        if (matches) (DialectReport.Accepted, "") else (DialectReport.Wrong, s"returned ${got.mkString("[", ", ", "]")}")
      } catch {
        case NonFatal(e) => (DialectReport.Rejected, rootMessage(e))
      }
    DialectReport.probe(engine, p.group, p.name, p.sql(identity), outcome, detail)
    println(f"[$engine] $outcome%-8s ${p.group.takeWhile(_ != '(').trim} / ${p.name}  $detail")
    (p.group, p.name) -> (outcome, detail)
  }.toMap

  private def accepted(group: String, name: String): Boolean = outcomes((group, name))._1 == DialectReport.Accepted

  private def requireAccepted(group: String, names: String*): Unit = names.foreach { n =>
    val (outcome, detail) = outcomes((group, n))
    assert(outcome == DialectReport.Accepted, s"$engine does not accept '$n' ($outcome) $detail")
  }

  private def bits(flags: (String, Boolean)*): String = {
    val on = flags.filter(_._2).map(_._1)
    if (on.isEmpty) "(none)" else on.mkString(", ")
  }

  test("dialect: syntax probe matrix") {
    outcomes
    val nullsFirst = (asc: Boolean) => {
      val dir = if (asc) "ASC" else "DESC"
      Try(withConnection(firstColumn(_, s"SELECT id FROM ${tableRef("sortable")} ORDER BY sort_key $dir, id")))
        .map(r => if (r.take(2) == Seq("2", "5")) "first" else "last").getOrElse("?")
    }
    DialectReport.info(engine, "NULLs sort by default (ASC / DESC)", s"${nullsFirst(true)} / ${nullsFirst(false)}")
    DialectReport.info(engine, "577 bits the engine could report", bits(
      "LIMIT" -> (accepted(LimitGroup, "LIMIT n") && accepted(LimitGroup, "LIMIT n OFFSET m")),
      "LIMIT (no OFFSET form)" -> (accepted(LimitGroup, "LIMIT n") && !accepted(LimitGroup, "LIMIT n OFFSET m")),
      "FETCH" -> accepted(LimitGroup, "OFFSET m ROWS FETCH NEXT n ROWS ONLY"),
      "TOP" -> accepted(LimitGroup, "SELECT TOP n")))
    DialectReport.info(engine, "578 bits the engine could report", bits(
      "FIRST_LAST" -> Seq("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST").forall(accepted(NullsGroup, _))))
    DialectReport.info(engine, "579 bits the engine could report", bits(
      "TRUE_FALSE" -> accepted(BoolGroup, "col = TRUE"),
      "INT_ONE_ZERO" -> accepted(BoolGroup, "col = 1")))
    DialectReport.info(engine, "580 bits, judged on DATE columns", bits(
      "ANSI_KEYWORD" -> accepted(DateTimeGroup, "date: DATE '...'"),
      "BARE_STRING" -> accepted(DateTimeGroup, "date: bare '...'")))
    DialectReport.info(engine, "580 bits, judged on TIMESTAMP columns", bits(
      "ANSI_KEYWORD" -> accepted(DateTimeGroup, "timestamp: TIMESTAMP '...', fractional seconds"),
      "BARE_STRING" -> accepted(DateTimeGroup, "timestamp: bare '...', fractional seconds")))
    DialectReport.info(engine, "LIKE escaping the engine could report", bits(
      "ESCAPE_CLAUSE" -> accepted(LikeGroup, "LIKE ... ESCAPE '!'"),
      "IMPLICIT_BACKSLASH" -> accepted(LikeGroup, "backslash escapes with no ESCAPE clause")))
    DialectReport.info(engine, "string literal traits the engine could report", bits(
      "BACKSLASH_ESCAPES" -> accepted(StringGroup, "backslash is an escape (\\\\ is one backslash)"),
      "NATIONAL_PREFIX_REQUIRED" -> !accepted(StringGroup, "non-ASCII literal")))
    DialectReport.info(engine, "timestamp with time zone literals the engine could report", bits(
      "KEYWORD_WITH_OFFSET" -> accepted(DateTimeGroup, "timestamptz: TIMESTAMP '... +05:30'"),
      "WITH_TIME_ZONE_KEYWORD" -> accepted(DateTimeGroup, "timestamptz: TIMESTAMP WITH TIME ZONE '... +05:30'"),
      "BARE_STRING_WITH_OFFSET" -> accepted(DateTimeGroup, "timestamptz: bare '... +05:30'")))
    DialectReport.info(engine, "connector dialect", dialect.productIterator.mkString(", "))
  }

  test("dialect: configured limit syntax is accepted") {
    dialect.limitOffsetSyntax match {
      case LimitOffsetSyntax.LimitOffset => requireAccepted(LimitGroup, "LIMIT n", "LIMIT n without ORDER BY")
      case LimitOffsetSyntax.OffsetFetch =>
        requireAccepted(LimitGroup, "OFFSET m ROWS FETCH NEXT n ROWS ONLY", "OFFSET/FETCH after ORDER BY (SELECT NULL)")
    }
  }

  test("dialect: configured null ordering syntax is accepted") {
    if (dialect.nullsOrderingSyntax == NullsOrderingSyntax.NullsFirstLast)
      requireAccepted(NullsGroup, "ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")
  }

  test("dialect: configured boolean literal is accepted") {
    assume(hasColumn("events", "active"), s"$engine has no boolean type")
    dialect.boolLiteral match {
      case BoolLiteral.TrueFalse => requireAccepted(BoolGroup, "col = TRUE", "col = FALSE")
      case BoolLiteral.IntOneZero => requireAccepted(BoolGroup, "col = 1", "col = 0")
    }
  }

  test("dialect: configured date/time literal is accepted") {
    dialect.dateTimeLiteral match {
      case DateTimeLiteral.AnsiKeyword =>
        requireAccepted(DateTimeGroup, "date: DATE '...'", "timestamp: TIMESTAMP '...', fractional seconds")
      case DateTimeLiteral.BareString =>
        requireAccepted(DateTimeGroup, "date: bare '...'", "timestamp: bare '...', fractional seconds")
    }
  }

  test("dialect: configured identifier quote is accepted") {
    dialect.identifierQuote match {
      case IdentifierQuote.DoubleQuote => requireAccepted(QuoteGroup, "double quotes")
      case IdentifierQuote.Backtick => requireAccepted(QuoteGroup, "backticks")
    }
  }

  test("dialect: configured string literal escaping is accepted") {
    requireAccepted(StringGroup, "'' is one quote")
    dialect.stringBackslash match {
      case StringBackslash.Literal => requireAccepted(StringGroup, "backslash is an ordinary character")
      case StringBackslash.Escape => requireAccepted(StringGroup, "backslash is an escape (\\\\ is one backslash)")
    }
    dialect.nonAsciiLiteral match {
      case NonAsciiLiteral.Plain => requireAccepted(StringGroup, "non-ASCII literal")
      case NonAsciiLiteral.NationalPrefix => requireAccepted(StringGroup, "N'...' national literal")
    }
  }

  test("dialect: configured LIKE escaping is accepted") {
    dialect.likeEscapeSyntax match {
      case LikeEscapeSyntax.EscapeClause => requireAccepted(LikeGroup, "LIKE ... ESCAPE '!'")
      case LikeEscapeSyntax.ImplicitBackslash => requireAccepted(LikeGroup, "backslash escapes with no ESCAPE clause")
    }
  }

  test("dialect: configured instant literal is accepted") {
    assume(hasColumn("tz_events", "ts_tz"), s"$engine has no timestamp with time zone type")
    dialect.instantLiteral match {
      case InstantLiteral.KeywordWithOffset => requireAccepted(DateTimeGroup, "timestamptz: TIMESTAMP '... +05:30'")
      case InstantLiteral.WithTimeZoneKeyword =>
        requireAccepted(DateTimeGroup, "timestamptz: TIMESTAMP WITH TIME ZONE '... +05:30'")
      case InstantLiteral.BareStringWithOffset => requireAccepted(DateTimeGroup, "timestamptz: bare '... +05:30'")
      case InstantLiteral.Unsupported =>
    }
  }

  // Emitted for every engine, whatever the dialect.
  test("dialect: aliased derived table and WHERE 1=0 are accepted") {
    requireAccepted(DerivedGroup, "(subquery) AS alias", "WHERE 1=0 around a derived table")
  }
}
