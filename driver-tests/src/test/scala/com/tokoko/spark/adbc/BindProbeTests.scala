package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.{AdbcException, AdbcStatusCode}
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtilsExtended

import java.time.{Instant, LocalDateTime}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Sends values as bound parameters instead of literals: which parameter markers each driver
 * accepts on a query, and whether a value of each type compares correctly once bound. The
 * outcomes go into the same report as the syntax probes.
 */
trait BindProbeTests { this: AdbcSuiteBase =>

  private val MarkerGroup = "Bind parameters: marker style"
  private val ResolveGroup = "Bind parameters: which bound column a marker takes"
  private val ValueGroup = "Bind parameters: value types, using the first accepted marker"
  private val ShapeGroup = "Bind parameters: positions and counts, using the first accepted marker"

  /**
   * Marker spellings, each a function of the parameter's position (from 1) and of the fixture
   * type it is compared with, because ClickHouse's marker names the type. Bound columns are
   * called p1, p2, ... for the spellings that refer to them by name.
   */
  private val markers: Seq[(String, (Int, ColType) => String)] = Seq(
    "?" -> ((_, _) => "?"),
    "$1" -> ((i, _) => s"$$$i"),
    ":1" -> ((i, _) => s":$i"),
    ":p1" -> ((i, _) => s":p$i"),
    "@p1" -> ((i, _) => s"@p$i"),
    "{p1:Type}" -> ((i, t) => s"{p$i:${sqlType(t).getOrElse("String")}}")
  )

  /**
   * Runs `sql` with one row of bound parameters and returns the first column of the result.
   *
   * Everything is allocated from one root allocator; a root from a different one cannot be
   * exported for the bind. Drivers built on the Rust adbc_ffi exporter (DataFusion, ClickHouse,
   * Exasol) borrow the bound schema without releasing it, and the JNI bridge doesn't release it
   * either, so with those the allocator reports a small leak when it closes. That is tolerated
   * here so that it doesn't mask the outcome of the query.
   */
  private def bound(sql: String, params: Seq[(DataType, Any)], names: Seq[String] = Nil): Seq[String] = {
    val schema = StructType(params.zipWithIndex.map { case ((dt, _), i) =>
      StructField(names.lift(i).getOrElse(s"p${i + 1}"), dt)
    })
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val db = AdbcDriverManager.getInstance().connect(adbcDriver, allocator, adbcParams.asJava)
      try {
        val conn = db.connect()
        try {
          val root = VectorSchemaRoot.create(ArrowUtilsExtended.toArrowSchema(schema, "UTC"), allocator)
          try {
            val writer = ArrowWriter.create(root)
            val row = Row.fromSeq(params.map(_._2))
            writer.write(CatalystTypeConverters.createToCatalystConverter(schema)(row).asInstanceOf[InternalRow])
            writer.finish()
            val stmt = conn.createStatement()
            try {
              stmt.setSqlQuery(sql)
              try stmt.prepare() catch {
                case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED =>
              }
              stmt.bind(root)
              readFirstColumn(stmt.executeQuery())
            } finally stmt.close()
          } finally root.close()
        } finally conn.close()
      } finally db.close()
    } finally {
      try allocator.close() catch { case _: IllegalStateException => }
    }
  }

  private def probe(group: String, name: String, requires: Option[(String, String)], sql: String, display: String,
      params: Seq[(DataType, Any)], expected: Seq[Int], names: Seq[String] = Nil): String = {
    val (outcome, detail) =
      if (requires.exists { case (table, column) => !hasColumn(table, column) }) (DialectReport.NotApplicable, "")
      else try {
        val got = bound(probeSql(sql), params, names)
        if (got == expected.map(_.toString)) (DialectReport.Accepted, "")
        else (DialectReport.Wrong, s"returned ${got.mkString("[", ", ", "]")}")
      } catch {
        case NonFatal(e) => (DialectReport.Rejected, rootMessage(e))
      }
    DialectReport.probe(engine, group, name, display, outcome, detail)
    println(f"[$engine] $outcome%-8s $group / $name  $detail")
    outcome
  }

  test("bind: parameter probe matrix") {
    val accepted = markers.filter { case (name, spell) =>
      val m = spell(1, ColType.Int32)
      probe(MarkerGroup, name, None, s"SELECT id FROM ${tableRef("sortable")} WHERE id = $m",
        s"SELECT id FROM sortable WHERE id = $name", Seq(IntegerType -> 3), Seq(3)) == DialectReport.Accepted
    }
    DialectReport.info(engine, "bind: parameter markers accepted on a query",
      if (accepted.isEmpty) "(none)" else accepted.map(_._1).mkString(", "))

    // The bound columns are (hi = 4, lo = 2) and the query mentions the lower bound first, so it
    // only returns rows if a numbered marker follows its number, or a named one its name, rather
    // than the order the markers appear in.
    Seq(
      ("$1", "numbered $n follows the number, not the order of appearance", "$2", "$1"),
      (":1", "numbered :n follows the number, not the order of appearance", ":2", ":1"),
      (":p1", "named :name follows the bound column's name", ":lo", ":hi"),
      ("@p1", "named @name follows the bound column's name", "@lo", "@hi")
    ).foreach { case (spelling, name, lo, hi) =>
      val requires = if (accepted.exists(_._1 == spelling)) None else Some("sortable" -> "(marker not accepted)")
      probe(ResolveGroup, name, requires,
        s"SELECT id FROM ${tableRef("sortable")} WHERE id >= $lo AND id <= $hi ORDER BY id",
        s"SELECT id FROM sortable WHERE id >= $lo AND id <= $hi, binding (hi = 4, lo = 2)",
        Seq(IntegerType -> 4, IntegerType -> 2), Seq(2, 3, 4), Seq("hi", "lo"))
    }

    // Without a working marker the other probes can't run; "?" keeps the report rows aligned.
    val marker = accepted.headOption.map(_._2).getOrElse((_: Int, _: ColType) => "?")
    def value(name: String, table: String, column: String, dataType: DataType, v: Any, expected: Int*): Unit = {
      val colType = Fixtures.byName(table).cols.find(_.name == column).get.tpe
      probe(ValueGroup, name, Some(table -> column),
        s"SELECT id FROM ${tableRef(table)} WHERE $column = ${marker(1, colType)} ORDER BY id",
        s"SELECT id FROM $table WHERE $column = <marker>", Seq(dataType -> v), expected)
    }

    value("int64 at its maximum", "big_ints", "v", LongType, Long.MaxValue, 1)
    value("string with a quote", "strings", "s", StringType, "O'Brien", 1)
    value("string with backslashes", "strings", "s", StringType, "C:\\temp\\new", 2)
    value("string outside ASCII", "strings", "s", StringType, "ქართული", 9)
    value("boolean", "events", "active", BooleanType, false, 2)
    value("date", "events", "event_date", DateType, java.sql.Date.valueOf("2024-06-20"), 2)
    value("timestamp with microseconds", "all_types", "c_ts", TimestampNTZType,
      LocalDateTime.parse("1999-12-31T23:59:59.999999"), 2)
    value("timestamp with time zone", "tz_events", "ts_tz", TimestampType,
      java.sql.Timestamp.from(Instant.parse("2024-06-20T23:59:59.999999Z")), 2)
    value("float32 with no exact binary form", "floats", "f32", FloatType, 1.1f, 1)
    value("float64 with no exact binary form", "floats", "f64", DoubleType, 0.1, 1)
    value("decimal(18,4)", "all_types", "c_dec", DecimalType(18, 4), new java.math.BigDecimal("1234.5678"), 1)
    value("binary", "all_types", "c_bin", BinaryType, Array(0xde, 0xad, 0xbe, 0xef).map(_.toByte), 4)

    def shape(name: String, sql: String, display: String, params: Seq[(DataType, Any)], expected: Int*): Unit =
      probe(ShapeGroup, name, None, sql, display, params, expected)
    val int = ColType.Int32
    val sortable = tableRef("sortable")

    shape("three parameters in an IN list",
      s"SELECT id FROM $sortable WHERE id IN (${marker(1, int)}, ${marker(2, int)}, ${marker(3, int)}) ORDER BY id",
      "SELECT id FROM sortable WHERE id IN (<m1>, <m2>, <m3>)",
      Seq(IntegerType -> 1, IntegerType -> 3, IntegerType -> 5), 1, 3, 5)
    shape("two parameters bounding a range",
      s"SELECT id FROM $sortable WHERE sort_key > ${marker(1, int)} AND sort_key <= ${marker(2, int)} ORDER BY id",
      "SELECT id FROM sortable WHERE sort_key > <m1> AND sort_key <= <m2>",
      Seq(IntegerType -> 10, IntegerType -> 30), 3, 4)
    // Equality with NULL is never true, so a driver that binds the NULL returns no rows.
    shape("a NULL parameter",
      s"SELECT id FROM $sortable WHERE sort_key = ${marker(1, int)} ORDER BY id",
      "SELECT id FROM sortable WHERE sort_key = <m1>", Seq(IntegerType -> null))

    val limitMarker = marker(1, ColType.Int64)
    val limited = dialect.limitOffsetSyntax match {
      case LimitOffsetSyntax.LimitOffset => s"LIMIT $limitMarker"
      case LimitOffsetSyntax.OffsetFetch => s"OFFSET 0 ROWS FETCH NEXT $limitMarker ROWS ONLY"
      case LimitOffsetSyntax.FetchFirst => s"FETCH FIRST $limitMarker ROWS ONLY"
    }
    shape("a parameter as the row limit",
      s"SELECT id FROM $sortable ORDER BY id $limited",
      "SELECT id FROM sortable ORDER BY id <the dialect's limit clause with a marker>", Seq(LongType -> 2L), 1, 2)
  }
}
