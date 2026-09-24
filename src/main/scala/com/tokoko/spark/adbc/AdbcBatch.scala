package com.tokoko.spark.adbc

import org.apache.arrow.adbc.core.{AdbcDatabase, AdbcException, AdbcStatusCode}
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class AdbcBatch(
    driver: String,
    params: Map[String, String],
    queries: Array[String],
    driverPartitioning: DriverPartitioning
) extends Batch {

  import AdbcBatch._

  // Planned once: with driver partitioning this executes every query on the database,
  // and the resulting descriptors must be the ones executors read.
  private lazy val partitions: Array[InputPartition] = driverPartitioning match {
    case DriverPartitioning.Disabled => queries.map(q => AdbcQueryPartition(q): InputPartition)
    case mode => planDriverPartitions(mode == DriverPartitioning.Required)
  }

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new AdbcPartitionReaderFactory(driver, params)
  }

  private def planDriverPartitions(required: Boolean): Array[InputPartition] = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val parameters: java.util.Map[String, Object] = params.view.mapValues(v => v: Object).toMap.asJava
      val database = AdbcDriverManager.getInstance().connect(driver, allocator, parameters)
      try {
        // Probe with the first query so an unsupported driver costs a single round-trip.
        executePartitioned(database, queries.head) match {
          case None if required =>
            throw new UnsupportedOperationException(
              s"driverPartitioning=required, but driver '$driver' does not implement executePartitioned")
          case None =>
            queries.map(q => AdbcQueryPartition(q): InputPartition)
          case Some(first) =>
            if (queries.length > 1) {
              log.info(s"Driver partitioning ${queries.length} range queries; all of them execute " +
                "on the database during planning")
            }
            val rest = executeAllPartitioned(database, queries.tail)
            (first +: rest).flatten.map(d => AdbcDescriptorPartition(d): InputPartition)
        }
      } finally database.close()
    } finally allocator.close()
  }

  private def executeAllPartitioned(database: AdbcDatabase, qs: Array[String]): Array[Seq[Array[Byte]]] = {
    if (qs.isEmpty) return Array.empty
    val pool = Executors.newFixedThreadPool(math.min(qs.length, MaxPlanningThreads))
    try {
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
      val futures = qs.toSeq.map { q =>
        Future {
          executePartitioned(database, q).getOrElse(throw new IllegalStateException(
            s"Driver '$driver' stopped supporting executePartitioned mid-planning"))
        }
      }
      Await.result(Future.sequence(futures), Duration.Inf).toArray
    } finally pool.shutdownNow()
  }

  /** Returns the query's partition descriptors, or None if the driver doesn't implement it. */
  private def executePartitioned(database: AdbcDatabase, query: String): Option[Seq[Array[Byte]]] = {
    val conn = database.connect()
    try {
      val stmt = conn.createStatement()
      try {
        stmt.setSqlQuery(query)
        val result = stmt.executePartitioned()
        Some(result.getPartitionDescriptors.asScala.toSeq.map { d =>
          val buf = d.getDescriptor.duplicate()
          val bytes = new Array[Byte](buf.remaining())
          buf.get(bytes)
          bytes
        })
      } finally stmt.close()
    } catch {
      case e: AdbcException if e.getStatus == AdbcStatusCode.NOT_IMPLEMENTED => None
    } finally conn.close()
  }
}

object AdbcBatch {
  private val log = LoggerFactory.getLogger(classOf[AdbcBatch])
  private val MaxPlanningThreads = 8
}
