package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDatabase, AdbcDriver, AdbcStatement, PartitionDescriptor}
import org.apache.arrow.adbc.driver.jni.JniDriverFactory
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * The JNI driver doesn't implement executePartitioned, so the driver-partitioned path is
 * exercised through [[FakePartitionedDriver]], which wraps JNI DuckDB and splits every
 * query into [[FakePartitionedDriver.PartitionsPerQuery]] descriptors.
 */
class AdbcDriverPartitioningTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val dbFile = new File("test-driver-partitioning.duckdb")
  private val jniFactory = "org.apache.arrow.adbc.driver.jni.JniDriverFactory"

  override def beforeAll(): Unit = {
    if (dbFile.exists()) dbFile.delete()
    val conn = DriverManager.getConnection(s"jdbc:duckdb:${dbFile.getAbsolutePath}")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE employees(id INTEGER NOT NULL, name VARCHAR, salary INTEGER NOT NULL)")
    stmt.execute("INSERT INTO employees VALUES (1, 'Tornike', 2000), (2, 'Robin', 3000), (3, 'Alice', 4000)")
    stmt.close()
    conn.close()

    FakePartitionedDriver.register()
    // AQE off so the executed plan exposes BatchScanExec directly
    spark = SparkSession.builder().master("local[2]")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (dbFile.exists()) dbFile.delete()
  }

  private def reader(driver: String): DataFrameReader =
    spark.read
      .format("com.tokoko.spark.adbc")
      .option("driver", driver)
      .option("jni.driver", "duckdb")
      .option("path", dbFile.getAbsolutePath)
      .option("dbtable", "employees")

  private def fakeReader: DataFrameReader = reader(FakePartitionedDriver.Name)

  private def withRange(r: DataFrameReader): DataFrameReader =
    r.option("partitionColumn", "id")
      .option("lowerBound", "1")
      .option("upperBound", "4")
      .option("numPartitions", "3")

  private def ids(df: org.apache.spark.sql.DataFrame): Seq[Int] =
    df.select("id").collect().map(_.getInt(0)).toSeq.sorted

  private def inputPartitions(df: org.apache.spark.sql.DataFrame): Seq[AdbcPartition] =
    df.queryExecution.executedPlan.collectFirst { case s: BatchScanExec => s }
      .get.inputPartitions.map(_.asInstanceOf[AdbcPartition])

  test("auto falls back to query partitions when driver lacks executePartitioned") {
    val df = reader(jniFactory).load()
    assert(ids(df) == Seq(1, 2, 3))
    assert(df.rdd.getNumPartitions == 1)
    assert(inputPartitions(df).forall(_.isInstanceOf[AdbcQueryPartition]))
  }

  test("required fails when driver lacks executePartitioned") {
    val e = intercept[Exception] {
      reader(jniFactory).option("driverPartitioning", "required").load().collect()
    }
    assert(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
      .exists(t => Option(t.getMessage).exists(_.contains("does not implement executePartitioned"))))
  }

  test("invalid driverPartitioning value is rejected") {
    intercept[IllegalArgumentException] {
      reader(jniFactory).option("driverPartitioning", "bogus").load().collect()
    }
  }

  test("driver partitioning is on by default and yields descriptor partitions") {
    val df = fakeReader.load()
    assert(ids(df) == Seq(1, 2, 3))
    val parts = inputPartitions(df)
    assert(parts.length == FakePartitionedDriver.PartitionsPerQuery)
    assert(parts.forall(_.isInstanceOf[AdbcDescriptorPartition]))
  }

  test("driverPartitioning=none uses a plain query") {
    val df = fakeReader.option("driverPartitioning", "none").load()
    assert(ids(df) == Seq(1, 2, 3))
    val parts = inputPartitions(df)
    assert(parts.length == 1)
    assert(parts.forall(_.isInstanceOf[AdbcQueryPartition]))
  }

  test("range partitioning disables driver partitioning by default") {
    val df = withRange(fakeReader).load()
    assert(ids(df) == Seq(1, 2, 3))
    assert(inputPartitions(df).length == 3)
    assert(inputPartitions(df).forall(_.isInstanceOf[AdbcQueryPartition]))
  }

  test("range and driver partitioning combine") {
    val df = withRange(fakeReader).option("driverPartitioning", "auto").load()
    val before = FakePartitionedDriver.executePartitionedCalls.get()
    val parts = inputPartitions(df)
    // one executePartitioned per range query, planned once
    assert(FakePartitionedDriver.executePartitionedCalls.get() - before == 3)
    assert(parts.length == 3 * FakePartitionedDriver.PartitionsPerQuery)
    assert(parts.forall(_.isInstanceOf[AdbcDescriptorPartition]))
    assert(df.count() == 3)
    assert(ids(df) == Seq(1, 2, 3))
  }

  test("aggregates are pushed down with driver partitioning") {
    val df = fakeReader.load().agg(sum("salary"), min("salary"), max("salary"))
    val row = df.collect().head
    assert(row.get(0).asInstanceOf[Number].longValue() == 9000)
    assert(row.get(1).asInstanceOf[Number].longValue() == 2000)
    assert(row.get(2).asInstanceOf[Number].longValue() == 4000)
    assert(inputPartitions(df).forall(_.isInstanceOf[AdbcDescriptorPartition]))
    assert(FakePartitionedDriver.lastQuery.get.contains("SUM("))
  }

  test("topN is pushed down and re-applied by Spark with driver partitioning") {
    val df = fakeReader.load().orderBy(col("salary").desc).limit(2)
    assert(df.collect().map(_.getAs[Int]("id")).toSeq == Seq(3, 2))
    assert(FakePartitionedDriver.lastQuery.get.contains("LIMIT 2"))
  }
}

/**
 * Test-only driver: delegates to JNI, but implements executePartitioned by returning
 * PartitionsPerQuery descriptors for each query. Descriptor 0 carries the query's full result,
 * the others are empty, so any duplicated or dropped descriptor shows up in row counts.
 */
object FakePartitionedDriver {
  val Name = "fake-partitioned"
  val PartitionsPerQuery = 2
  val executePartitionedCalls = new AtomicInteger()
  @volatile var lastQuery: Option[String] = None

  def register(): Unit =
    AdbcDriverManager.getInstance().registerDriver(Name, (alloc: BufferAllocator) => driver(alloc))

  private def driver(alloc: BufferAllocator): AdbcDriver = new AdbcDriver {
    private val jni = new JniDriverFactory().getDriver(alloc)
    override def open(params: java.util.Map[String, Object]): AdbcDatabase = new Database(jni.open(params))
  }

  private class Database(delegate: AdbcDatabase) extends AdbcDatabase {
    override def connect(): AdbcConnection = new Connection(delegate.connect())
    override def close(): Unit = delegate.close()
  }

  private class Connection(delegate: AdbcConnection) extends AdbcConnection {
    private val opened = ArrayBuffer.empty[AdbcStatement]

    override def createStatement(): AdbcStatement = new Statement(delegate.createStatement())
    override def getInfo(infoCodes: Array[Int]): ArrowReader = delegate.getInfo(infoCodes)

    override def readPartition(descriptor: ByteBuffer): ArrowReader = {
      val text = UTF_8.decode(descriptor).toString
      val Array(index, query) = text.split("\u0000", 2)
      val sql = if (index == "0") query else s"SELECT * FROM ($query) AS t WHERE 1=0"
      val stmt = delegate.createStatement()
      opened += stmt
      stmt.setSqlQuery(sql)
      stmt.executeQuery().getReader
    }

    override def close(): Unit = {
      opened.foreach(_.close())
      delegate.close()
    }
  }

  private class Statement(delegate: AdbcStatement) extends AdbcStatement {
    private var query: String = _

    override def setSqlQuery(q: String): Unit = {
      query = q
      delegate.setSqlQuery(q)
    }
    override def executeQuery(): AdbcStatement.QueryResult = delegate.executeQuery()
    override def executeSchema(): org.apache.arrow.vector.types.pojo.Schema = delegate.executeSchema()
    override def executeUpdate(): AdbcStatement.UpdateResult = delegate.executeUpdate()
    override def prepare(): Unit = delegate.prepare()
    override def close(): Unit = delegate.close()

    override def executePartitioned(): AdbcStatement.PartitionResult = {
      executePartitionedCalls.incrementAndGet()
      lastQuery = Some(query)
      val descriptors = (0 until PartitionsPerQuery).map { i =>
        new PartitionDescriptor(ByteBuffer.wrap(s"$i\u0000$query".getBytes(UTF_8)))
      }
      new AdbcStatement.PartitionResult(null, -1, descriptors.asJava)
    }
  }
}
