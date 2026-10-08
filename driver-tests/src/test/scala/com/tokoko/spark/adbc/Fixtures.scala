package com.tokoko.spark.adbc

import org.apache.spark.sql.types._

import java.time.{Instant, LocalDate, LocalDateTime}

/** Engine-independent column types; each suite maps them to native type names. */
sealed trait ColType
object ColType {
  case object Int16 extends ColType
  case object Int32 extends ColType
  case object Int64 extends ColType
  case object Float32 extends ColType
  case object Float64 extends ColType
  case class Decimal(precision: Int, scale: Int) extends ColType
  case object Str extends ColType
  case object Bool extends ColType
  case object Date extends ColType
  /** Wall-clock timestamp, microsecond precision. */
  case object Timestamp extends ColType
  /** Instant (timestamp with time zone), microsecond precision. */
  case object TimestampTz extends ColType
  case object Binary extends ColType
}

case class Col(name: String, tpe: ColType, nullable: Boolean = true)

/** Row values are Short/Int/Long/Float/Double/java.math.BigDecimal/String/Boolean/LocalDate/LocalDateTime/Instant/Array[Byte]/null. */
case class TableSpec(name: String, cols: Seq[Col], rows: Seq[Seq[Any]])

object Fixtures {
  import ColType._

  private def dec(s: String) = new java.math.BigDecimal(s)
  private def date(s: String) = LocalDate.parse(s)
  private def ts(s: String) = LocalDateTime.parse(s)
  private def bytes(xs: Int*) = xs.map(_.toByte).toArray

  private val employeeCols = Seq(Col("id", Int32, nullable = false), Col("name", Str), Col("salary", Int32, nullable = false))

  val employees: TableSpec = TableSpec("employees", employeeCols, Seq(
    Seq(1, "Tornike", 2000),
    Seq(2, "Robin", 3000),
    Seq(3, "Alice", 4000)
  ))

  val writeTarget: TableSpec = TableSpec("write_target", employeeCols, Nil)

  val reservedKw: TableSpec = TableSpec("reserved_kw",
    Seq(Col("id", Int32, nullable = false), Col("order", Int32, nullable = false)),
    Seq(Seq(1, 10), Seq(2, 20), Seq(3, 30)))

  val events: TableSpec = TableSpec("events",
    Seq(Col("id", Int32, nullable = false), Col("event_date", Date, nullable = false), Col("active", Bool, nullable = false)),
    Seq(
      Seq(1, date("2024-01-15"), true),
      Seq(2, date("2024-06-20"), false),
      Seq(3, date("2025-03-10"), true)
    ))

  val sortable: TableSpec = TableSpec("sortable",
    Seq(Col("id", Int32, nullable = false), Col("sort_key", Int32)),
    Seq(Seq(1, 10), Seq(2, null), Seq(3, 30), Seq(4, 20), Seq(5, null)))

  private val allTypesCols = Seq(
    Col("id", Int32, nullable = false),
    Col("c_small", Int16),
    Col("c_int", Int32),
    Col("c_big", Int64),
    Col("c_real", Float32),
    Col("c_double", Float64),
    Col("c_dec", Decimal(18, 4)),
    Col("c_str", Str),
    Col("c_bool", Bool),
    Col("c_date", Date),
    Col("c_ts", Timestamp),
    Col("c_bin", Binary)
  )

  // Float/double values are exactly representable so engines can't disagree on rounding.
  // c_big stays within 2^53; the full 64-bit range has its own table, big_ints.
  val allTypes: TableSpec = TableSpec("all_types", allTypesCols, Seq(
    Seq(1, 1.toShort, 100, 1000000000000L, 1.5f, 2.5, dec("1234.5678"), "alpha", true,
      date("2024-01-15"), ts("2024-01-15T10:30:00"), bytes(0x01, 0x02)),
    Seq(2, (-1).toShort, -100, -1000000000000L, -1.5f, -2.5, dec("-1234.5678"), "Beta", false,
      date("1999-12-31"), ts("1999-12-31T23:59:59.999999"), bytes(0xff, 0x00)),
    Seq(3, 0.toShort, 0, 0L, 0.0f, 0.0, dec("0.0000"), "gamma", true,
      date("1970-01-01"), ts("1970-01-01T00:00:00"), bytes(0x00)),
    Seq(4, Short.MaxValue, Int.MaxValue, 9007199254740991L, 3.25f, 1.0E10, dec("99999999999999.9999"), "ქართული", false,
      date("2038-01-19"), ts("2038-01-19T03:14:07.123456"), bytes(0xde, 0xad, 0xbe, 0xef)),
    Seq(5, Short.MinValue, Int.MinValue, -9007199254740991L, 0.125f, 1.0E-5, dec("0.0001"), "it's", true,
      date("2000-02-29"), ts("2000-02-29T12:00:00.5"), bytes(0x7f)),
    Seq(6, null, null, null, null, null, null, null, null, null, null, null)
  ))

  val writeTypes: TableSpec = TableSpec("write_types", allTypesCols, Nil)

  val strings: TableSpec = TableSpec("strings",
    Seq(Col("id", Int32, nullable = false), Col("s", Str)),
    Seq(
      Seq(1, "O'Brien"),
      // \t and \n are escapes wherever backslash escapes at all
      Seq(2, "C:\\temp\\new"),
      Seq(3, "100%"),
      Seq(4, "under_score"),
      Seq(5, "bang!"),
      Seq(6, "Robin"),
      Seq(7, "robin"),
      Seq(8, "ROBIN"),
      Seq(9, "ქართული"),
      Seq(10, null),
      Seq(11, "underXscore"),
      Seq(12, "100 percent"),
      Seq(13, "double\"quote"),
      Seq(14, "trail"),
      Seq(15, "trail ")
    ))

  val tzEvents: TableSpec = TableSpec("tz_events",
    Seq(Col("id", Int32, nullable = false), Col("ts_tz", TimestampTz)),
    Seq(
      Seq(1, Instant.parse("2024-01-15T10:30:00Z")),
      Seq(2, Instant.parse("2024-06-20T23:59:59.999999Z")),
      Seq(3, Instant.parse("2025-03-10T00:00:00Z")),
      Seq(4, null)
    ))

  val quirkyNames: TableSpec = TableSpec("quirky_names",
    Seq(Col("id", Int32, nullable = false), Col("MixedCase", Int32), Col("with space", Int32), Col("select", Int32)),
    Seq(Seq(1, 10, 100, 1000), Seq(2, 20, 200, 2000), Seq(3, 30, 300, 3000)))

  // Decimals narrow enough for Arrow's 32- and 64-bit decimal layouts.
  val smallDecimals: TableSpec = TableSpec("small_decimals",
    Seq(Col("id", Int32, nullable = false), Col("d9", Decimal(9, 2)), Col("d17", Decimal(17, 4))),
    Seq(
      Seq(1, dec("12.50"), dec("1234.5678")),
      Seq(2, dec("-0.01"), dec("0.0001")),
      Seq(3, dec("9999999.99"), dec("-9999999999999.9999")),
      Seq(4, null, null)
    ))

  // 64-bit integers a double cannot hold exactly.
  val bigInts: TableSpec = TableSpec("big_ints",
    Seq(Col("id", Int32, nullable = false), Col("v", Int64)),
    // Seq[Any], or Scala widens the Int ids to Long alongside the Long values.
    Seq(Seq[Any](1, Long.MaxValue), Seq[Any](2, -Long.MaxValue), Seq[Any](3, 9007199254740993L), Seq[Any](4, null)))

  val all: Seq[TableSpec] = Seq(employees, writeTarget, reservedKw, events, sortable, allTypes, writeTypes,
    strings, tzEvents, quirkyNames, smallDecimals, bigInts)

  def byName(name: String): TableSpec = all.find(_.name == name).getOrElse(sys.error(s"no fixture table $name"))

  def sparkType(t: ColType): DataType = t match {
    case Int16 => ShortType
    case Int32 => IntegerType
    case Int64 => LongType
    case Float32 => FloatType
    case Float64 => DoubleType
    case Decimal(p, s) => DecimalType(p, s)
    case Str => StringType
    case Bool => BooleanType
    case Date => DateType
    case Timestamp => TimestampNTZType
    case TimestampTz => TimestampType
    case Binary => BinaryType
  }
}
