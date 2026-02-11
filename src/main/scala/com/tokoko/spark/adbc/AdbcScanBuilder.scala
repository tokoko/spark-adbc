package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.expressions.{NamedReference, SortDirection, NullOrdering, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownTopN}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ByteType, ShortType, IntegerType, FloatType, DoubleType, LongType, StructField, StructType}

class AdbcScanBuilder(
    schema: StructType,
    driver: String,
    params: Map[String, String],
    dbtable: Option[String],
    query: Option[String]
) extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters
  with SupportsPushDownLimit
  with SupportsPushDownTopN
  with SupportsPushDownAggregates {

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
    pushedLimit = Some(limit)
    true
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    val supported = orders.forall(_.expression().isInstanceOf[NamedReference])
    if (supported) {
      pushedOrders = orders
      pushedLimit = Some(limit)
      true
    } else {
      false
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
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

    val whereClause = if (pushedFilterArray.nonEmpty) {
      " WHERE " + pushedFilterArray.map(FilterConverter.convert).mkString(" AND ")
    } else ""

    val (selectClause, groupByClause, outputSchema) = pushedAggregation match {
      case Some(agg) =>
        val groupByCols = agg.groupByExpressions().map {
          case ref: NamedReference => ref.fieldNames().head
        }
        val aggCols = agg.aggregateExpressions().map(convertAggFunc)
        val select = (groupByCols ++ aggCols).mkString(", ")
        val groupBy = if (groupByCols.nonEmpty) {
          " GROUP BY " + groupByCols.mkString(", ")
        } else ""
        (select, groupBy, buildAggregateSchema(agg))

      case None =>
        val cols = if (prunedSchema.fields.nonEmpty) {
          prunedSchema.fields.map(_.name).mkString(", ")
        } else "*"
        (cols, "", prunedSchema)
    }

    val orderByClause = if (pushedOrders.nonEmpty) {
      " ORDER BY " + pushedOrders.map(convertSortOrder).mkString(", ")
    } else ""

    val limitClause = pushedLimit.map(l => s" LIMIT $l").getOrElse("")

    val finalQuery = s"SELECT $selectClause FROM $baseRelation$whereClause$groupByClause$orderByClause$limitClause"

    new AdbcScan(driver, outputSchema, params, finalQuery)
  }

  private def columnName(expr: org.apache.spark.sql.connector.expressions.Expression): String = {
    expr.asInstanceOf[NamedReference].fieldNames().head
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

  private def buildAggregateSchema(agg: Aggregation): StructType = {
    val groupByFields = agg.groupByExpressions().map {
      case ref: NamedReference => schema(ref.fieldNames().head)
    }
    val aggFields = agg.aggregateExpressions().map {
      case _: CountStar => StructField("count(*)", LongType, nullable = false)
      case c: Count =>
        StructField(s"count(${columnName(c.column())})", LongType, nullable = false)
      case s: Sum =>
        val col = columnName(s.column())
        val sumType = schema(col).dataType match {
          case ByteType | ShortType | IntegerType | LongType => LongType
          case FloatType | DoubleType => DoubleType
          case other => other
        }
        StructField(s"sum($col)", sumType)
      case m: Min =>
        val col = columnName(m.column())
        StructField(s"min($col)", schema(col).dataType)
      case m: Max =>
        val col = columnName(m.column())
        StructField(s"max($col)", schema(col).dataType)
      case a: Avg =>
        StructField(s"avg(${columnName(a.column())})", DoubleType)
    }
    StructType(groupByFields ++ aggFields)
  }

  private def convertSortOrder(order: SortOrder): String = {
    val name = order.expression().asInstanceOf[NamedReference].fieldNames().mkString(".")
    val dir = order.direction() match {
      case SortDirection.ASCENDING => "ASC"
      case SortDirection.DESCENDING => "DESC"
    }
    val nulls = order.nullOrdering() match {
      case NullOrdering.NULLS_FIRST => " NULLS FIRST"
      case NullOrdering.NULLS_LAST => " NULLS LAST"
    }
    s"$name $dir$nulls"
  }
}
