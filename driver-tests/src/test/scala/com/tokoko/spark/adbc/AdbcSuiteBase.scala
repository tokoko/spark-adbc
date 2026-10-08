package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, Tag}
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.funsuite.AnyFunSuite

import java.math.{MathContext, RoundingMode}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

sealed trait Pushed
object Pushed {
  case object Filter extends Pushed
  case object Limit extends Pushed
  case object TopN extends Pushed
  case object Aggregate extends Pushed
}

/**
 * Per-engine plumbing shared by every driver suite: starts the database, creates the
 * [[Fixtures]] tables in the engine's native types, and offers `checkSame`, which runs the
 * same DataFrame query through the connector and over an in-memory copy of the fixture
 * and requires both to agree.
 */
abstract class AdbcSuiteBase extends AnyFunSuite with BeforeAndAfterAll {

  protected val jniFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"
  protected var spark: SparkSession = _

  /** Driver name as passed to `jni.driver`; also the column name in the dialect report. */
  protected def engine: String
  protected def adbcParams: Map[String, Object]
  protected def adbcDriver: String = jniFactory

  protected def startDatabase(): Unit = ()
  protected def stopDatabase(): Unit = ()

  /** Native type name for a fixture column type, or None if the engine has no such type. */
  protected def sqlType(t: ColType): Option[String]

  /** Test name -> reason, for tests known to fail on this engine. They are cancelled, not failed. */
  protected def knownGaps: Map[String, String] = Map.empty

  /** The dialect the connector picks for this engine. */
  protected def dialect: SqlDialect = SqlDialect.fromOptions(None, Some(engine))

  protected def adbcReader: DataFrameReader =
    adbcParams.foldLeft(spark.read.format("com.tokoko.spark.adbc").option("driver", adbcDriver)) {
      case (r, (k, v)) => r.option(k, v.toString)
    }

  /** How a fixture table is referenced in SQL and in the `dbtable` option. */
  protected def tableRef(name: String): String = name

  protected def load(table: String, options: Map[String, String] = Map.empty): DataFrame =
    adbcReader.option("dbtable", tableRef(table)).options(options).load()

  // ---- fixture setup ----

  protected def quoteId(name: String): String = SqlBuilder.quoteId(dialect, name)

  protected def columnDdl(c: Col, nativeType: String): String =
    s"${quoteId(c.name)} $nativeType" + (if (c.nullable) "" else " NOT NULL")

  protected def createTableSuffix: String = ""

  protected val setupTimestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  protected def hex(b: Array[Byte]): String = b.map(x => f"${x & 0xff}%02X").mkString

  /** Literal used when inserting fixture rows; deliberately independent of FilterConverter. */
  protected def setupLiteral(v: Any, t: ColType): String = v match {
    case null => "NULL"
    case s: String => "'" + s.replace("'", "''") + "'"
    case b: Boolean => if (b) "TRUE" else "FALSE"
    case d: LocalDate => s"'$d'"
    case ts: LocalDateTime => s"'${ts.format(setupTimestampFormat)}'"
    case i: Instant => s"'${LocalDateTime.ofInstant(i, ZoneOffset.UTC).format(setupTimestampFormat)}+00:00'"
    case b: Array[Byte] => s"X'${hex(b)}'"
    case d: java.math.BigDecimal => d.toPlainString
    case f: Float => new java.math.BigDecimal(f.toString).toPlainString
    case d: Double => new java.math.BigDecimal(d.toString).toPlainString
    case other => other.toString
  }

  protected def withConnection[A](f: AdbcConnection => A): A = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val db = AdbcDriverManager.getInstance().connect(adbcDriver, allocator, adbcParams.asJava)
      try {
        val conn = db.connect()
        try f(conn) finally conn.close()
      } finally db.close()
    } finally allocator.close()
  }

  protected def execSetup(conn: AdbcConnection, sql: String): Unit = {
    val stmt = conn.createStatement()
    try {
      stmt.setSqlQuery(sql)
      stmt.executeUpdate()
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"[$engine] setup statement failed: $sql", e)
    } finally stmt.close()
  }

  protected def supportedCols(spec: TableSpec): Seq[Col] = spec.cols.filter(c => sqlType(c.tpe).isDefined)

  protected def hasColumn(table: String, column: String): Boolean =
    supportedCols(Fixtures.byName(table)).exists(_.name == column)

  protected def createTable(spec: TableSpec): Unit = withConnection { conn =>
    val keep = spec.cols.indices.filter(i => sqlType(spec.cols(i).tpe).isDefined)
    val ddl = keep.map(i => columnDdl(spec.cols(i), sqlType(spec.cols(i).tpe).get)).mkString(", ")
    execSetup(conn, s"CREATE TABLE ${spec.name}($ddl)$createTableSuffix")
    if (spec.rows.nonEmpty) {
      val values = spec.rows.map(r => keep.map(i => setupLiteral(r(i), spec.cols(i).tpe)).mkString("(", ", ", ")"))
      execSetup(conn, s"INSERT INTO ${spec.name} VALUES ${values.mkString(", ")}")
    }
  }

  override def beforeAll(): Unit = {
    startDatabase()
    spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    Fixtures.all.foreach(createTable)
  }

  override def afterAll(): Unit = {
    try DialectReport.flush(engine)
    finally {
      if (spark != null) spark.stop()
      stopDatabase()
    }
  }

  // ---- known gaps ----

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit =
    super.test(testName, testTags: _*) {
      knownGaps.get(testName) match {
        case None => testFun
        case Some(reason) =>
          val failure = try { testFun; None } catch {
            case e: TestCanceledException => throw e
            case NonFatal(e) => Some(e)
          }
          failure match {
            case Some(e) =>
              DialectReport.gap(engine, testName, reason, rootMessage(e))
              cancel(s"known gap: $reason")
            case None => fail(s"listed in knownGaps but passes now, remove it: $reason")
          }
      }
    }(pos)

  protected def rootMessage(e: Throwable): String = {
    var root = e
    while (root.getCause != null && root.getCause != root) root = root.getCause
    val head = Option(e.getMessage).getOrElse(e.getClass.getSimpleName).linesIterator.take(3).mkString(" ")
    val tail = if (root eq e) "" else " <- " + Option(root.getMessage).getOrElse(root.getClass.getSimpleName).linesIterator.take(3).mkString(" ")
    (head + tail).take(600)
  }

  // ---- differential checking ----

  private val schemaCache = mutable.Map[String, StructType]()
  private val referenceCache = mutable.Map[String, DataFrame]()

  /** Schema the connector infers for a fixture table. */
  protected def adbcSchema(table: String): StructType = schemaCache.getOrElseUpdate(table, load(table).schema)

  protected def frameOf(spec: TableSpec, schema: StructType): DataFrame = {
    val idx = schema.fields.map(f => spec.cols.indexWhere(_.name == f.name))
    val rows = spec.rows.map { r =>
      Row.fromSeq(idx.zip(schema.fields).toSeq.map { case (i, f) => sparkValue(r(i), f.dataType) })
    }
    spark.createDataFrame(rows.asJava, schema)
  }

  protected def fixtureSchema(spec: TableSpec): StructType =
    StructType(supportedCols(spec).map(c => StructField(c.name, Fixtures.sparkType(c.tpe), c.nullable)))

  /**
   * In-memory copy of a fixture table, the oracle for `checkSame`. Wall-clock timestamp
   * columns follow the driver when it reports them as instants (ClickHouse), read as UTC.
   */
  protected def reference(table: String): DataFrame = referenceCache.getOrElseUpdate(table, {
    val spec = Fixtures.byName(table)
    val actual = adbcSchema(table)
    val schema = StructType(fixtureSchema(spec).map { f =>
      val asInstant = f.dataType == TimestampNTZType &&
        actual.find(_.name == f.name).exists(_.dataType == TimestampType)
      if (asInstant) f.copy(dataType = TimestampType) else f
    })
    frameOf(spec, schema)
  })

  private def sparkValue(v: Any, dt: DataType): Any = (v, dt) match {
    case (null, _) => null
    case (l: LocalDateTime, TimestampType) => java.sql.Timestamp.from(l.toInstant(ZoneOffset.UTC))
    case (i: Instant, _) => java.sql.Timestamp.from(i)
    case (d: LocalDate, _) => java.sql.Date.valueOf(d)
    case _ => v
  }

  /** Timestamp literal matching the flavour (wall-clock or instant) of a fixture column. */
  protected def tsLit(table: String, column: String, value: String): Column = {
    val ldt = LocalDateTime.parse(value)
    if (reference(table).schema(column).dataType == TimestampType) lit(java.sql.Timestamp.from(ldt.toInstant(ZoneOffset.UTC)))
    else lit(ldt)
  }

  private def family(dt: DataType): String = dt match {
    case ByteType | ShortType | IntegerType | LongType => "integer"
    case FloatType | DoubleType => "floating point"
    case _: DecimalType => "decimal"
    case _: StringType => "string"
    case other => other.simpleString
  }

  /** Cancels the test unless the driver exposes the columns with the fixture's kind of type. */
  protected def requireNative(table: String, columns: String*): Unit = columns.foreach { c =>
    assume(hasColumn(table, c), s"$engine has no native type for $table.$c")
    val actual = adbcSchema(table)(c).dataType
    val expected = reference(table).schema(c).dataType
    assume(family(actual) == family(expected),
      s"driver exposes $table.$c as ${actual.simpleString}, not ${expected.simpleString}")
  }

  protected def pushedSql(df: DataFrame): Seq[String] =
    df.queryExecution.optimizedPlan.collect { case r: DataSourceV2ScanRelation => r.scan }
      .collect { case s: AdbcScan => s.queries.toSeq }.flatten

  protected def assertPushed(df: DataFrame, pushed: Set[Pushed]): Unit = {
    val plan = df.queryExecution.optimizedPlan
    val sql = pushedSql(df)
    def shown = s"\n  pushed SQL: ${sql.mkString("; ")}\n$plan"
    pushed.foreach {
      case Pushed.Filter =>
        assert(plan.collect { case f: logical.Filter => f }.isEmpty, s"filter was not pushed down$shown")
      case Pushed.Aggregate =>
        assert(plan.collect { case a: logical.Aggregate => a }.isEmpty, s"aggregate was not pushed down$shown")
      case Pushed.TopN =>
        assert(sql.nonEmpty && sql.forall(_.contains(" ORDER BY ")), s"topN was not pushed down$shown")
      case Pushed.Limit =>
        assert(sql.nonEmpty && sql.forall(s => s.contains(" LIMIT ") || s.contains(" FETCH NEXT ")),
          s"limit was not pushed down$shown")
    }
  }

  private val canonTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  private def exact(v: Any, scale: Option[Int]): String = {
    val bd = v match {
      case d: java.math.BigDecimal => d
      case b: Boolean => if (b) java.math.BigDecimal.ONE else java.math.BigDecimal.ZERO
      case other => new java.math.BigDecimal(other.toString.trim)
    }
    scale.map(bd.setScale(_, RoundingMode.HALF_UP)).getOrElse(bd).stripTrailingZeros.toPlainString
  }

  private def double(v: Any): Double = v match {
    case n: Number => n.doubleValue
    case other => other.toString.trim.toDouble
  }

  /**
   * Renders a value by the type Spark computes for it, so drivers that surface the same
   * value differently (INT as BIGINT, DECIMAL with another scale) still compare equal.
   */
  private def canon(v: Any, expected: DataType): String =
    if (v == null) "NULL"
    else Try(expected match {
      case BooleanType => v match {
        case b: Boolean => b.toString
        case n: Number => (n.doubleValue != 0).toString
        case other => other.toString.toLowerCase
      }
      case ByteType | ShortType | IntegerType | LongType => exact(v, None)
      case d: DecimalType => exact(v, Some(d.scale))
      case FloatType => double(v).toFloat.toString
      case DoubleType =>
        val d = double(v)
        if (d.isNaN || d.isInfinite) d.toString
        else new java.math.BigDecimal(d).round(new MathContext(12)).stripTrailingZeros.toPlainString
      case DateType => v match {
        case d: java.sql.Date => d.toString
        case d: LocalDate => d.toString
        case other => other.toString.take(10)
      }
      case TimestampNTZType => v match {
        case l: LocalDateTime => l.format(canonTimestamp)
        case t: java.sql.Timestamp => LocalDateTime.ofInstant(t.toInstant, ZoneOffset.UTC).format(canonTimestamp)
        case other => LocalDateTime.parse(other.toString.trim.replace(' ', 'T')).format(canonTimestamp)
      }
      case TimestampType => v match {
        case t: java.sql.Timestamp => t.toInstant.toString
        case i: Instant => i.toString
        case l: LocalDateTime => l.toInstant(ZoneOffset.UTC).toString
        case other => other.toString
      }
      case BinaryType => v match {
        case b: Array[Byte] => hex(b)
        case other => other.toString
      }
      case _ => v.toString
    }).getOrElse(s"<$v: ${v.getClass.getSimpleName}>")

  /** Requires both frames to hold the same rows, compared by the types of `expectedDf`. */
  protected def assertSameRows(expectedDf: DataFrame, actualDf: DataFrame, ordered: Boolean = false): Unit = {
    val types = expectedDf.schema.fields.map(_.dataType)
    def render(rows: Array[Row]): Seq[String] = rows.toSeq.map { r =>
      if (r.length != types.length) s"<row with ${r.length} columns>"
      else types.indices.map(i => canon(r.get(i), types(i))).mkString(" | ")
    }
    val sql = pushedSql(actualDf).mkString("; ")
    val expected = render(expectedDf.collect())
    val actual = try render(actualDf.collect()) catch {
      case NonFatal(e) => fail(s"query failed: ${rootMessage(e)}\n  pushed SQL: $sql", e)
    }
    println(s"[$engine] $sql")
    val (e, a) = if (ordered) (expected, actual) else (expected.sorted, actual.sorted)
    if (e != a) {
      fail(s"result differs from Spark's own evaluation\n  pushed SQL: $sql" +
        s"\n  expected (${e.size}):\n    ${e.mkString("\n    ")}\n  actual (${a.size}):\n    ${a.mkString("\n    ")}")
    }
    assert(actualDf.columns.map(_.toLowerCase).toSeq == expectedDf.columns.map(_.toLowerCase).toSeq)
  }

  /**
   * Runs `q` through the connector and over the in-memory reference and requires identical
   * rows, then requires that the listed operators really were pushed to the database.
   */
  protected def checkSame(table: String, ordered: Boolean = false, pushed: Set[Pushed] = Set.empty,
      options: Map[String, String] = Map.empty)(q: DataFrame => DataFrame): Unit = {
    val actualDf = q(load(table, options))
    assertSameRows(q(reference(table)), actualDf, ordered)
    assertPushed(actualDf, pushed)
  }
}
