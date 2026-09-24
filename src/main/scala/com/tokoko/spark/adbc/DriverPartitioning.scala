package com.tokoko.spark.adbc

/**
 * Whether each generated query is executed via ADBC `executePartitioned`, letting the
 * driver split its result into multiple Spark partitions. Orthogonal to client-driven
 * range partitioning: with both enabled, every range query is split further.
 */
sealed trait DriverPartitioning

object DriverPartitioning {
  /** Always run queries with `executeQuery` on the executors. */
  case object Disabled extends DriverPartitioning
  /** Try `executePartitioned`; fall back to plain queries if the driver doesn't implement it. */
  case object Auto extends DriverPartitioning
  /** Use `executePartitioned` and fail if the driver doesn't implement it. */
  case object Required extends DriverPartitioning

  /** Driver partitioning defaults to `auto`, unless client-driven range partitioning is configured. */
  def fromOption(value: Option[String], rangePartitioned: Boolean): DriverPartitioning =
    value.map(_.toLowerCase) match {
      case None => if (rangePartitioned) Disabled else Auto
      case Some("none") => Disabled
      case Some("auto") => Auto
      case Some("required") => Required
      case Some(other) => throw new IllegalArgumentException(
        s"Invalid driverPartitioning '$other'; expected one of: none, auto, required")
    }
}
