package com.tokoko.spark.adbc

import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connector.expressions.{NamedReference, SortDirection, NullOrdering, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownTopN}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExtended

import scala.jdk.CollectionConverters._

class AdbcScanBuilder(
    schema: StructType,
    driver: String,
    params: Map[String, String],
    dbtable: Option[String],
    query: Option[String],
    dialect: SqlDialect = SqlDialect.Default,
    partitionColumn: Option[String] = None,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None,
    numPartitions: Option[Int] = None
) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters
  with SupportsPushDownLimit
  with SupportsPushDownTopN
  with SupportsPushDownAggregates {

  private val isPartitioned: Boolean = partitionColumn.isDefined

  private var prunedSchema: StructType = schema
  private var pushedFilterArray: Array[Filter] = Array.empty
  private var pushedLimit: Option[Int] = None
  private var pushedOrders: Array[SortOrder] = Array.empty
  private var pushedAggregation: Option[Aggregation] = None

  override def pruneColumns(requiredSchema: StructType): Unit = {
    prunedSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition(FilterConverter.canConvert)
    pushedFilterArray = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = pushedFilterArray

  override def isPartiallyPushed(): Boolean = false

  override def pushLimit(limit: Int): Boolean = {
    if (isPartitioned) return false
    pushedLimit = Some(limit)
    true
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (isPartitioned) return false
    if (!orders.forall(_.expression().isInstanceOf[NamedReference])) return false
    if (dialect.nullsOrderingSyntax == NullsOrderingSyntax.Unsupported) {
      // Dialects without NULLS FIRST/LAST syntax (MySQL, MSSQL) treat NULLs as
      // smallest values: default is NULLS_FIRST for ASC, NULLS_LAST for DESC
      // (matches Spark's default). Reject the pushdown if Spark asked for the
      // opposite and the sort column can actually contain NULLs.
      val needsExplicitNulls = orders.exists { o =>
        val name = o.expression().asInstanceOf[NamedReference].fieldNames().head
        val nullable = schema.fields.find(_.name == name).forall(_.nullable)
        val default = o.direction() match {
          case SortDirection.ASCENDING => NullOrdering.NULLS_FIRST
          case SortDirection.DESCENDING => NullOrdering.NULLS_LAST
        }
        nullable && o.nullOrdering() != default
      }
      if (needsExplicitNulls) return false
    }
    pushedOrders = orders
    pushedLimit = Some(limit)
    true
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (isPartitioned) return false
    val groupBySupported = aggregation.groupByExpressions().forall(_.isInstanceOf[NamedReference])
    if (!groupBySupported) return false

    val aggsSupported = aggregation.aggregateExpressions().forall {
      case _: CountStar => true
      case c: Count => c.column().isInstanceOf[NamedReference]
      case s: Sum => s.column().isInstanceOf[NamedReference]
      case m: Min => m.column().isInstanceOf[NamedReference]
      case m: Max => m.column().isInstanceOf[NamedReference]
      case a: Avg => a.column().isInstanceOf[NamedReference]
      case _ => false
    }
    if (!aggsSupported) return false

    pushedAggregation = Some(aggregation)
    true
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = true

  override def build(): Scan = {
    val baseRelation = dbtable match {
      case Some(table) => table
      case None => s"(${query.get}) AS T"
    }

    val filterPredicates = pushedFilterArray.map(f => FilterConverter.convert(f, dialect)).toSeq

    val (selectClause, groupByClause) = pushedAggregation match {
      case Some(agg) =>
        val groupByCols = agg.groupByExpressions().map {
          case ref: NamedReference => SqlBuilder.quoteId(dialect, ref.fieldNames().head)
        }
        val aggCols = agg.aggregateExpressions().zipWithIndex.map {
          case (a, i) => s"${convertAggFunc(a)} AS agg_$i"
        }
        val select = (groupByCols ++ aggCols).mkString(", ")
        val groupBy = if (groupByCols.nonEmpty) {
          " GROUP BY " + groupByCols.mkString(", ")
        } else ""
        (select, groupBy)

      case None =>
        val cols = if (prunedSchema.fields.nonEmpty) {
          prunedSchema.fields.map(f => SqlBuilder.quoteId(dialect, f.name)).mkString(", ")
        } else "*"
        (cols, "")
    }

    val orderByClause = if (pushedOrders.nonEmpty) {
      " ORDER BY " + pushedOrders.map(convertSortOrder).mkString(", ")
    } else if (pushedLimit.isDefined && SqlBuilder.requiresOrderByForLimit(dialect)) {
      " ORDER BY (SELECT NULL)"
    } else ""

    val limitClause = SqlBuilder.limitClause(dialect, pushedLimit)

    val queries: Array[String] = if (isPartitioned) {
      generatePartitionQueries(selectClause, baseRelation, filterPredicates)
    } else {
      val whereClause = if (filterPredicates.nonEmpty) {
        " WHERE " + filterPredicates.mkString(" AND ")
      } else ""
      Array(s"SELECT $selectClause FROM $baseRelation$whereClause$groupByClause$orderByClause$limitClause")
    }

    val outputSchema = if (pushedAggregation.isDefined) {
      inferSchema(s"SELECT $selectClause FROM $baseRelation$groupByClause")
    } else {
      prunedSchema
    }

    new AdbcScan(driver, outputSchema, params, queries)
  }

  private def generatePartitionQueries(
      selectClause: String,
      baseRelation: String,
      filterPredicates: Seq[String]
  ): Array[String] = {
    val col = SqlBuilder.quoteId(dialect, partitionColumn.get)
    val lower = lowerBound.get
    val upper = upperBound.get
    val numParts = numPartitions.get

    if (numParts == 1) {
      val whereClause = if (filterPredicates.nonEmpty) " WHERE " + filterPredicates.mkString(" AND ") else ""
      return Array(s"SELECT $selectClause FROM $baseRelation$whereClause")
    }

    val stride = (BigDecimal(upper) - BigDecimal(lower)) / BigDecimal(numParts)

    (0 until numParts).map { i =>
      val partitionPredicate = if (i == 0) {
        val bound = (BigDecimal(lower) + stride).toLong
        s"($col < $bound OR $col IS NULL)"
      } else if (i == numParts - 1) {
        val bound = (BigDecimal(lower) + stride * i).toLong
        s"$col >= $bound"
      } else {
        val lBound = (BigDecimal(lower) + stride * i).toLong
        val uBound = (BigDecimal(lower) + stride * (i + 1)).toLong
        s"$col >= $lBound AND $col < $uBound"
      }

      val allPredicates = Seq(partitionPredicate) ++ filterPredicates
      val whereClause = " WHERE " + allPredicates.mkString(" AND ")

      s"SELECT $selectClause FROM $baseRelation$whereClause"
    }.toArray
  }

  private def inferSchema(query: String): StructType = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val parameters: java.util.Map[String, Object] = params.view.mapValues(v => v: Object).toMap.asJava
      val database = AdbcDriverManager.getInstance().connect(driver, allocator, parameters)
      try {
        val conn = database.connect()
        try ArrowUtilsExtended.fromArrowSchema(SchemaInference.run(conn, query))
        finally conn.close()
      } finally database.close()
    } finally allocator.close()
  }

  private def columnName(expr: org.apache.spark.sql.connector.expressions.Expression): String = {
    SqlBuilder.quoteId(dialect, expr.asInstanceOf[NamedReference].fieldNames().head)
  }

  private def convertAggFunc(func: AggregateFunc): String = func match {
    case _: CountStar => "COUNT(*)"
    case c: Count if c.isDistinct => s"COUNT(DISTINCT ${columnName(c.column())})"
    case c: Count => s"COUNT(${columnName(c.column())})"
    case s: Sum if s.isDistinct => s"SUM(DISTINCT ${columnName(s.column())})"
    case s: Sum => s"SUM(${columnName(s.column())})"
    case m: Min => s"MIN(${columnName(m.column())})"
    case m: Max => s"MAX(${columnName(m.column())})"
    case a: Avg if a.isDistinct => s"AVG(DISTINCT ${columnName(a.column())})"
    case a: Avg => s"AVG(${columnName(a.column())})"
  }

  private def convertSortOrder(order: SortOrder): String = {
    val name = SqlBuilder.quoteId(dialect, order.expression().asInstanceOf[NamedReference].fieldNames().head)
    val dir = order.direction() match {
      case SortDirection.ASCENDING => "ASC"
      case SortDirection.DESCENDING => "DESC"
    }
    SqlBuilder.formatSortOrder(dialect, name, dir, order.nullOrdering())
  }
}
