package com.tokoko.spark.adbc

/**
 * Reasons used in the suites' `knownGaps`. Each is a way pushed-down SQL goes wrong on some
 * engine today; they are grouped by what would have to change for the test to pass.
 */
object Gaps {

  // ---- semantic differences: the SQL is accepted but means something else than in Spark ----

  val Collation = "semantics: column collation is case-insensitive or pads trailing spaces, Spark compares bytes"
  val SortCollation = "semantics: engine orders strings by a linguistic collation, Spark by bytes"
  val IntegerAvg = "semantics: AVG over an integer column returns an integer"
  val IntSumOverflow = "semantics: SUM over INT is computed as INT and overflows, Spark widens to BIGINT"

  // ---- type mapping problems between driver, Arrow Java and Spark ----

  val NarrowDecimal = "types: driver returns decimal32/decimal64, which Arrow Java 18 reads as 128-bit garbage"
  val WideDecimal = "types: aggregate result is a decimal wider than Spark's maximum precision of 38"
  val UnsignedCount = "types: COUNT returns UInt64, which Spark has no Arrow mapping for"
  val BooleanAsTinyint = "types: BOOLEAN is read back as tinyint"
  val IngestTypes = "types: driver bulk ingest rejects or mangles one of the fixture column types"
}
