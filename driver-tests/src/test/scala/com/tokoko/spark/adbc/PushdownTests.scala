package com.tokoko.spark.adbc

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import java.time.Instant

/** Reads of every fixture column type, compared with Spark's own copy of the data. */
trait DataTypeTests { this: AdbcSuiteBase with CoreTests =>

  test("types: full table read") {
    checkSame("all_types")(identity)
  }

  test("types: Spark type mapping") {
    Seq("all_types", "tz_events").foreach { table =>
      val spec = Fixtures.byName(table)
      val schema = adbcSchema(table)
      spec.cols.filter(_.name != "id").foreach { c =>
        sqlType(c.tpe) match {
          case Some(native) => DialectReport.typeMapping(engine, c.tpe.toString, native, schema(c.name).dataType.simpleString)
          case None => DialectReport.typeMapping(engine, c.tpe.toString, "(no such type)", "–")
        }
      }
      assert(schema.fieldNames.toSeq == supportedCols(spec).map(_.name))
    }
  }

  test("types: each column on its own") {
    supportedCols(Fixtures.allTypes).filter(_.name != "id").foreach { c =>
      checkSame("all_types")(_.select("id", c.name))
    }
  }

  test("types: decimals narrower than 64 bits") {
    checkSame("small_decimals")(identity)
  }

  test("types: timestamp with time zone") {
    assume(hasColumn("tz_events", "ts_tz"), s"$engine has no timestamp with time zone type")
    checkSame("tz_events")(identity)
  }

  test("types: range-partitioned read") {
    val options = Map("partitionColumn" -> "id", "lowerBound" -> "1", "upperBound" -> "6", "numPartitions" -> "3")
    checkSame("all_types", options = options)(identity)
    checkSame("all_types", options = options)(_.filter("c_int > 0"))
  }

  test("write: all types round trip") {
    assume(supportsWrite, s"writes are not set up for $engine")
    writeTo(reference("all_types"), "write_types")
    assertSameRows(reference("all_types"), load("write_types"))
  }
}

/**
 * One filter per literal kind the connector can render. Each must return what Spark returns
 * and must really reach the database, so a literal the engine misreads shows up as a diff.
 */
trait LiteralPushdownTests { this: AdbcSuiteBase =>

  private def filterTest(name: String, table: String, columns: String*)(cond: => Column): Unit =
    test(name) {
      requireNative(table, columns: _*)
      // Comparisons against instants stay in Spark when the dialect has no literal for them.
      val instants = columns.exists(c => reference(table).schema(c).dataType == TimestampType)
      val pushable = !(instants && dialect.instantLiteral == InstantLiteral.Unsupported)
      checkSame(table, pushed = if (pushable) Set(Pushed.Filter) else Set.empty)(_.filter(cond))
    }

  // ---- numbers ----

  filterTest("literal int: equality", "all_types", "c_int")(expr("c_int = 100"))
  filterTest("literal int: negative", "all_types", "c_int")(expr("c_int < -50"))
  filterTest("literal int: IN list", "all_types", "c_int")(expr("c_int IN (100, -100, 0)"))
  filterTest("literal smallint: lower bound", "all_types", "c_small")(expr("c_small = -32768"))
  filterTest("literal bigint: upper bound", "all_types", "c_big")(expr("c_big = 9223372036854775807"))
  filterTest("literal float: equality", "all_types", "c_real")(expr("c_real = 1.5"))
  filterTest("literal double: comparison", "all_types", "c_double")(expr("c_double > 2.4"))
  filterTest("literal double: large exponent", "all_types", "c_double")(expr("c_double >= 1.0E10"))
  filterTest("literal double: small exponent", "all_types", "c_double")(expr("c_double = 1.0E-5"))
  filterTest("literal decimal: equality", "all_types", "c_dec")(expr("c_dec = 1234.5678"))
  filterTest("literal decimal: full precision", "all_types", "c_dec")(expr("c_dec >= 99999999999999.9999"))
  filterTest("literal decimal: negative", "all_types", "c_dec")(expr("c_dec < -0.5"))

  // ---- strings ----

  filterTest("literal string: equality", "strings", "s")(col("s") === "Robin")
  filterTest("literal string: single quote", "strings", "s")(col("s") === "O'Brien")
  filterTest("literal string: backslash", "strings", "s")(col("s") === "C:\\temp\\new")
  filterTest("literal string: double quote", "strings", "s")(col("s") === "double\"quote")
  filterTest("literal string: non-ASCII", "strings", "s")(col("s") === "ქართული")
  filterTest("literal string: equality is case sensitive", "strings", "s")(col("s") === "robin")
  filterTest("literal string: trailing space is significant", "strings", "s")(col("s") === "trail")
  filterTest("literal string: IN list", "strings", "s")(col("s").isin("O'Brien", "100%", "nope"))
  filterTest("literal string: range comparison", "strings", "s")(col("s") >= "a")

  filterTest("LIKE: prefix containing %", "strings", "s")(col("s").startsWith("100%"))
  filterTest("LIKE: contains _", "strings", "s")(col("s").contains("_"))
  filterTest("LIKE: suffix containing the escape character", "strings", "s")(col("s").endsWith("g!"))
  filterTest("LIKE: contains backslash", "strings", "s")(col("s").contains("\\"))
  filterTest("LIKE: prefix containing a quote", "strings", "s")(col("s").startsWith("O'"))
  filterTest("LIKE: prefix is case sensitive", "strings", "s")(col("s").startsWith("rob"))

  // ---- booleans ----

  filterTest("literal boolean: = true", "all_types", "c_bool")(col("c_bool") === true)
  filterTest("literal boolean: = false", "all_types", "c_bool")(col("c_bool") === false)

  test("literal boolean: bare column and negation") {
    requireNative("all_types", "c_bool")
    checkSame("all_types")(_.filter(col("c_bool")))
    checkSame("all_types")(_.filter(!col("c_bool")))
  }

  // ---- dates and timestamps ----

  filterTest("literal date: equality", "all_types", "c_date")(expr("c_date = date '2024-01-15'"))
  filterTest("literal date: comparison", "all_types", "c_date")(expr("c_date > date '2000-02-28'"))
  filterTest("literal date: before the epoch boundary", "all_types", "c_date")(expr("c_date <= date '1970-01-01'"))
  filterTest("literal date: IN list", "all_types", "c_date")(expr("c_date IN (date '1999-12-31', date '2038-01-19')"))
  filterTest("literal date: range", "all_types", "c_date")(
    expr("c_date >= date '2000-01-01' AND c_date < date '2025-01-01'"))

  filterTest("literal timestamp: equality with microseconds", "all_types", "c_ts")(
    col("c_ts") === tsLit("all_types", "c_ts", "1999-12-31T23:59:59.999999"))
  filterTest("literal timestamp: comparison", "all_types", "c_ts")(
    col("c_ts") > tsLit("all_types", "c_ts", "2024-01-15T10:29:59"))
  filterTest("literal timestamp: sub-second boundary", "all_types", "c_ts")(
    col("c_ts") > tsLit("all_types", "c_ts", "2000-02-29T12:00:00.4") &&
      col("c_ts") < tsLit("all_types", "c_ts", "2000-02-29T12:00:00.6"))
  filterTest("literal timestamp: IN list", "all_types", "c_ts")(
    col("c_ts").isin(tsLit("all_types", "c_ts", "1970-01-01T00:00:00"), tsLit("all_types", "c_ts", "2038-01-19T03:14:07.123456")))

  filterTest("literal timestamp with time zone: comparison", "tz_events", "ts_tz")(
    col("ts_tz") > lit(java.sql.Timestamp.from(Instant.parse("2024-06-20T23:00:00Z"))))
  filterTest("literal timestamp with time zone: equality", "tz_events", "ts_tz")(
    col("ts_tz") === lit(java.sql.Timestamp.from(Instant.parse("2024-06-20T23:59:59.999999Z"))))

  // ---- binary ----

  test("literal binary: equality stays in Spark") {
    requireNative("all_types", "c_bin")
    checkSame("all_types")(_.filter(col("c_bin") === lit(Array(0xde, 0xad, 0xbe, 0xef).map(_.toByte))))
  }

  // ---- NULL handling and boolean structure ----

  filterTest("null: IS NULL", "all_types")(col("c_int").isNull)
  filterTest("null: IS NOT NULL", "all_types")(col("c_str").isNotNull)
  filterTest("null: NOT excludes NULL rows", "all_types", "c_int")(!(col("c_int") === 100))
  filterTest("null: NOT IN excludes NULL rows", "all_types", "c_int")(!col("c_int").isin(100, -100))
  filterTest("logic: OR across columns", "all_types", "c_int", "c_str")(col("c_int") === 100 || col("c_str") === "Beta")
  filterTest("logic: nested AND / OR / NOT", "all_types", "c_int", "c_str")(
    (col("c_int") > 0 && !(col("c_str") === "alpha")) || (col("c_int").isNull && col("id") === 6))

  test("null: null-safe equality stays in Spark") {
    checkSame("all_types")(_.filter(col("c_int") <=> 100))
    checkSame("all_types")(_.filter(col("c_int") <=> lit(null)))
  }

  // ---- identifiers ----

  test("identifiers: full read of mixed-case, spaced and keyword columns") {
    checkSame("quirky_names")(identity)
  }

  test("identifiers: filter on mixed-case, spaced and keyword columns") {
    checkSame("quirky_names", pushed = Set(Pushed.Filter))(
      _.filter(col("MixedCase") > 10 && col("with space") < 300 && col("select") === 2000))
  }

  test("identifiers: order by and aggregate on quoted names") {
    checkSame("quirky_names", ordered = true)(_.orderBy(col("select").desc).limit(2))
    checkSame("quirky_names", pushed = Set(Pushed.Aggregate))(_.groupBy("MixedCase").agg(sum(col("with space"))))
  }
}

/** LIMIT and ORDER BY ... LIMIT in every direction / null placement the connector can emit. */
trait OrderLimitTests { this: AdbcSuiteBase =>

  private def nullsSyntax: Boolean = dialect.nullsOrderingSyntax == NullsOrderingSyntax.NullsFirstLast

  private def topNTest(name: String, table: String, expectPushed: => Boolean = true, requires: Seq[String] = Nil)
      (q: DataFrame => DataFrame): Unit =
    test(name) {
      requireNative(table, requires: _*)
      checkSame(table, ordered = true, pushed = if (expectPushed) Set(Pushed.TopN) else Set.empty)(q)
    }

  test("limit: plain") {
    val df = load("all_types").limit(3)
    assert(df.collect().length == 3)
    assertPushed(df, Set(Pushed.Limit))
  }

  test("limit: with filter") {
    val df = load("all_types").filter("id > 2").limit(2)
    val ids = df.collect().map(_.getAs[Number]("id").intValue)
    assert(ids.length == 2 && ids.forall(_ > 2))
    assertPushed(df, Set(Pushed.Limit, Pushed.Filter))
  }

  test("limit: over query option") {
    val df = adbcReader.option("query", s"SELECT id, c_int FROM ${tableRef("all_types")} WHERE id <= 4").load().limit(2)
    assert(df.collect().length == 2)
    assertPushed(df, Set(Pushed.Limit))
  }

  test("limit: larger than the table") {
    assert(load("all_types").limit(100).collect().length == 6)
  }

  test("limit: with offset") {
    checkSame("all_types", ordered = true)(_.select("id", "c_int").orderBy("id").offset(2).limit(3))
  }

  topNTest("topN: ascending, Spark default null placement", "sortable")(
    _.orderBy(col("sort_key").asc, col("id").asc).limit(4))
  topNTest("topN: descending, Spark default null placement", "sortable")(
    _.orderBy(col("sort_key").desc, col("id").asc).limit(4))
  topNTest("topN: ascending nulls last", "sortable", nullsSyntax)(
    _.orderBy(col("sort_key").asc_nulls_last, col("id").asc).limit(4))
  topNTest("topN: descending nulls first", "sortable", nullsSyntax)(
    _.orderBy(col("sort_key").desc_nulls_first, col("id").asc).limit(4))
  topNTest("topN: two nullable keys in opposite directions", "all_types")(
    _.select("id", "c_bool", "c_int").orderBy(col("c_bool").desc, col("c_int").asc, col("id").asc).limit(4))
  topNTest("topN: by date", "all_types")(_.select("id", "c_date").orderBy(col("c_date").desc, col("id").asc).limit(3))
  topNTest("topN: by timestamp", "all_types")(_.select("id", "c_ts").orderBy(col("c_ts").asc, col("id").asc).limit(3))
  topNTest("topN: by decimal", "all_types", requires = Seq("c_dec"))(_.select("id", "c_dec").orderBy(col("c_dec").desc, col("id").asc).limit(3))
  topNTest("topN: by double", "all_types")(_.select("id", "c_double").orderBy(col("c_double").asc, col("id").asc).limit(4))
  topNTest("topN: by string", "employees")(_.orderBy(col("name").asc, col("id").asc).limit(2))
  topNTest("topN: string order matches Spark", "strings")(_.orderBy(col("s").asc, col("id").asc).limit(8))
  topNTest("topN: with filter and column pruning", "all_types")(
    _.select("id", "c_int").filter("c_int > -200").orderBy(col("c_int").desc, col("id").asc).limit(2))
}

/** Aggregate pushdown: results and result types must match what Spark computes itself. */
trait AggregateTests { this: AdbcSuiteBase =>

  private def aggTest(name: String, table: String, columns: String*)(q: DataFrame => DataFrame): Unit =
    test(name) {
      requireNative(table, columns: _*)
      checkSame(table, pushed = Set(Pushed.Aggregate))(q)
    }

  aggTest("agg: count star", "all_types")(_.agg(count("*")))
  aggTest("agg: count column skips nulls", "all_types")(_.agg(count("c_int"), count("c_str"), count("id")))
  aggTest("agg: count distinct", "sortable")(_.agg(countDistinct("sort_key")))
  aggTest("agg: sum of integers", "all_types")(_.filter("id <= 3").agg(sum("c_small"), sum("c_int"), sum("c_big")))
  aggTest("agg: sum(int) beyond the int range", "all_types")(_.filter("id IN (1, 4)").agg(sum("c_int")))
  aggTest("agg: sum of decimal and double", "all_types", "c_dec")(_.agg(sum("c_dec"), sum("c_double")))
  aggTest("agg: avg of int keeps the fraction", "sortable")(_.filter("id <= 2").agg(avg("id")))
  aggTest("agg: avg of decimal and double", "all_types", "c_dec")(_.filter("id IN (1, 3)").agg(avg("c_dec"), avg("c_double")))
  aggTest("agg: min and max of numbers", "all_types")(
    _.agg(min("c_int"), max("c_int"), min("c_big"), max("c_big"), min("c_double"), max("c_double")))
  aggTest("agg: min and max of decimal", "all_types", "c_dec")(_.agg(min("c_dec"), max("c_dec")))
  aggTest("agg: min and max of date and timestamp", "all_types", "c_date", "c_ts")(
    _.agg(min("c_date"), max("c_date"), min("c_ts"), max("c_ts")))
  aggTest("agg: min and max of string", "employees")(_.agg(min("name"), max("name")))
  aggTest("agg: group by boolean", "all_types", "c_bool")(_.groupBy("c_bool").agg(count("*"), sum("c_small")))
  aggTest("agg: group by nullable key", "sortable")(_.groupBy("sort_key").agg(count("*")))
  aggTest("agg: group by two columns", "all_types")(_.groupBy("c_bool", "c_str").agg(max("c_int")))
  aggTest("agg: group by date", "events")(_.groupBy("event_date").agg(count("*")))
  aggTest("agg: group by string is case sensitive", "strings")(_.groupBy("s").agg(count("*")))
  aggTest("agg: over empty input", "all_types")(_.filter("id < 0").agg(count("*"), sum("c_int"), max("c_date")))
  aggTest("agg: with filter", "all_types")(_.filter("c_int > 0").agg(count("*"), min("c_date"), sum("c_double")))

  test("agg: over query option") {
    val df = adbcReader.option("query", s"SELECT * FROM ${tableRef("sortable")} WHERE sort_key IS NOT NULL").load()
      .agg(sum("sort_key"), count("*"))
    val row = df.collect().head
    assert(row.getAs[Number](0).longValue == 60 && row.getAs[Number](1).longValue == 3)
    assertPushed(df, Set(Pushed.Aggregate))
  }
}
