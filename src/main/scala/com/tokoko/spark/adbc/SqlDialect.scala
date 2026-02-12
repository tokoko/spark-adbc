package com.tokoko.spark.adbc

import org.apache.spark.sql.connector.expressions.NullOrdering

trait SqlDialect {
  def selectPrefix(limit: Option[Int]): String
  def limitSuffix(limit: Option[Int]): String
  def formatSortOrder(name: String, dir: String, nullOrdering: NullOrdering): String
  def schemaInferenceQuery(selectClause: String, baseRelation: String, groupByClause: String): String
}

object SqlDialect {
  def apply(name: String): SqlDialect = name.toLowerCase match {
    case "sqlite" => SqliteDialect
    case "mssql" => MssqlDialect
    case _ => DefaultDialect
  }

  def fromOptions(dialect: Option[String], jniDriver: Option[String]): SqlDialect =
    apply(dialect.getOrElse(jniDriver.getOrElse("default")))
}

object DefaultDialect extends SqlDialect {
  override def selectPrefix(limit: Option[Int]): String = ""

  override def limitSuffix(limit: Option[Int]): String =
    limit.map(l => s" LIMIT $l").getOrElse("")

  override def formatSortOrder(name: String, dir: String, nullOrdering: NullOrdering): String = {
    val nulls = nullOrdering match {
      case NullOrdering.NULLS_FIRST => " NULLS FIRST"
      case NullOrdering.NULLS_LAST => " NULLS LAST"
    }
    s"$name $dir$nulls"
  }

  override def schemaInferenceQuery(selectClause: String, baseRelation: String, groupByClause: String): String =
    s"SELECT $selectClause FROM $baseRelation WHERE 1=0$groupByClause"
}

object SqliteDialect extends SqlDialect {
  override def selectPrefix(limit: Option[Int]): String = ""

  override def limitSuffix(limit: Option[Int]): String =
    limit.map(l => s" LIMIT $l").getOrElse("")

  override def formatSortOrder(name: String, dir: String, nullOrdering: NullOrdering): String = {
    val nulls = nullOrdering match {
      case NullOrdering.NULLS_FIRST => " NULLS FIRST"
      case NullOrdering.NULLS_LAST => " NULLS LAST"
    }
    s"$name $dir$nulls"
  }

  override def schemaInferenceQuery(selectClause: String, baseRelation: String, groupByClause: String): String =
    s"SELECT $selectClause FROM $baseRelation WHERE 1=1$groupByClause LIMIT 1"
}

object MssqlDialect extends SqlDialect {
  override def selectPrefix(limit: Option[Int]): String =
    limit.map(l => s"TOP $l ").getOrElse("")

  override def limitSuffix(limit: Option[Int]): String = ""

  override def formatSortOrder(name: String, dir: String, nullOrdering: NullOrdering): String =
    s"$name $dir"

  override def schemaInferenceQuery(selectClause: String, baseRelation: String, groupByClause: String): String =
    s"SELECT $selectClause FROM $baseRelation WHERE 1=0$groupByClause"
}
