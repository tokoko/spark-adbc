# CLAUDE.md

## Project Overview

spark-adbc is a Spark DataSource V2 connector for Apache Arrow ADBC (Arrow Database Connectivity). It enables Spark SQL to read from and write to ADBC-compatible databases using Arrow's columnar format for efficient data transfer.

## Build System

This project uses **SBT** (Scala Build Tool) with a multi-project build.

- **Build:** `pixi run sbt compile`
- **Test (unit):** `pixi run sbt test`
- **Test (driver integration):** `pixi run sbt driverTests/test`
- **Package:** `pixi run sbt package`
- **Clean:** `pixi run sbt clean`

## Tech Stack

- **Java:** 17
- **Language:** Scala 2.13.17
- **Framework:** Apache Spark 4.1.1 (DataSource V2 API)
- **Protocol:** Apache Arrow ADBC 0.22.0
- **Test framework:** ScalaTest (FunSuite style)
- **Test databases:** SQLite (embedded), DuckDB (embedded), PostgreSQL (testcontainers), MSSQL (testcontainers)

## Project Structure

```
src/main/scala/
  com/tokoko/spark/adbc/       # Main connector implementation
    DefaultSource.scala         # Entry point (TableProvider), schema inference
    AdbcTable.scala             # Table (SupportsRead, SupportsWrite)
    AdbcScanBuilder.scala       # Read path: scan builder with pushdowns
    AdbcScan.scala              # Read path: scan
    AdbcBatch.scala             # Read path: batch/partition planning
    AdbcPartitionReaderFactory.scala
    AdbcPartitionReader.scala   # Read path: executes ADBC query
    AdbcPartition.scala         # Partition descriptor
    AdbcWriteBuilder.scala      # Write path: write builder
    AdbcWrite.scala             # Write path: write
    AdbcBatchWrite.scala        # Write path: batch write
    AdbcDataWriterFactory.scala
    AdbcDataWriter.scala        # Write path: buffers rows, bulk inserts
    AdbcWriterCommitMessage.scala
    SqlDialect.scala            # SQL dialect abstraction (Default, SQLite, MSSQL)
    FilterConverter.scala       # Spark filter -> SQL WHERE conversion
  org/apache/spark/sql/util/
    ArrowUtilsExtended.scala    # Arrow <-> Spark format conversion utilities
src/test/scala/
  com/tokoko/spark/adbc/
    AdbcCometTest.scala         # Comet integration test (requires PostgreSQL)
    AdbcJdbcBenchmarkTest.scala # ADBC vs JDBC benchmark (requires PostgreSQL)
driver-tests/                   # Separate subproject for driver integration tests
  src/test/scala/
    com/tokoko/spark/adbc/
      AdbcTestBase.scala        # Abstract base test suite (18 tests)
      AdbcSqliteTest.scala      # SQLite driver tests
      AdbcDuckdbTest.scala      # DuckDB driver tests
      AdbcPostgresTest.scala    # PostgreSQL driver tests (testcontainers)
      AdbcMssqlTest.scala       # MSSQL driver tests (testcontainers)
```

## Architecture

The connector follows the Spark DataSource V2 pattern with factory/builder layers:

- **Read path:** `DefaultSource` -> `AdbcTable` -> `AdbcScanBuilder` -> `AdbcScan` -> `AdbcBatch` -> `AdbcPartitionReaderFactory` -> `AdbcPartitionReader`
- **Write path:** `DefaultSource` -> `AdbcTable` -> `AdbcWriteBuilder` -> `AdbcWrite` -> `AdbcBatchWrite` -> `AdbcDataWriterFactory` -> `AdbcDataWriter`

Users configure the connector with options: `driver` (ADBC driver class), `uri` (database URI), `dialect` (SQL dialect), and either `dbtable` or `query`. Client-driven partitioning is supported via `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` options.

### SQL Dialect Support

The `SqlDialect` trait abstracts database-specific SQL differences. Set via the `dialect` option:

- **`default`** — ANSI SQL: `LIMIT`, `NULLS FIRST/LAST`, `WHERE 1=0` for schema inference
- **`sqlite`** — Like default but uses `LIMIT 1` for schema inference (SQLite needs real data to infer aggregate types)
- **`mssql`** — T-SQL: `TOP N` instead of `LIMIT`, no `NULLS FIRST/LAST`, `WHERE 1=0` for schema inference

### Pushdown Support

The connector supports: column pruning, filter pushdown, limit pushdown, topN pushdown, and aggregate pushdown (COUNT, SUM, MIN, MAX, AVG). Aggregate output schemas are inferred by running the actual query shape against the database rather than hardcoding types.

### Client-Driven Partitioning

Range-based partitioning splits reads across N Spark partitions using a numeric column. Options: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` (all four required together). Follows the same stride-based approach as Spark's JDBC connector. When partitioning is active, aggregation/limit/topN pushdowns are disabled (Spark handles them after collecting all partitions).

## Code Conventions

- 2-space indentation
- PascalCase for classes, camelCase for methods
- Package: `com.tokoko.spark.adbc`
- No formatter configured (no .scalafmt.conf)
- Tests extend `AnyFunSuite` with `BeforeAndAfterAll`

## Testing

- **Unit tests** (`src/test/`): Comet integration and JDBC benchmarks (require external PostgreSQL via Docker)
- **Driver tests** (`driver-tests/`): 72 tests across 4 databases (SQLite, DuckDB, PostgreSQL, MSSQL) validating reads, pushdowns, aggregates, and partitioned reads. PostgreSQL and MSSQL use testcontainers (require Docker). DuckDB uses `path` option instead of `uri`.

Run driver tests: `pixi run sbt driverTests/test`
