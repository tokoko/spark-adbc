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
- **Protocol:** Apache Arrow ADBC 0.23.0
- **Test framework:** ScalaTest (FunSuite style)
- **Test databases:** DuckDB, DataFusion, SQLite (embedded); PostgreSQL, MySQL, MSSQL, ClickHouse, Trino, Presto, SingleStore, Exasol, GizmoSQL via Flight SQL, Spark Connect (testcontainers)

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
    AdbcPartition.scala         # Partition types: query or driver descriptor
    DriverPartitioning.scala    # driverPartitioning option (none/auto/required)
    AdbcWriteBuilder.scala      # Write path: write builder
    AdbcWrite.scala             # Write path: write
    AdbcBatchWrite.scala        # Write path: batch write
    AdbcDataWriterFactory.scala
    AdbcDataWriter.scala        # Write path: buffers rows, bulk inserts
    AdbcWriterCommitMessage.scala
    SqlDialect.scala            # SQL dialect flags (Default, MSSQL, MySQL, SingleStore, ClickHouse, DataFusion, Trino, Spark)
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
      Fixtures.scala            # Engine-independent fixture tables (types, strings, nulls, ...)
      AdbcSuiteBase.scala       # Per-engine plumbing: DDL from fixtures, checkSame, knownGaps
      AdbcTestBase.scala        # Full suite = CoreTests + the traits below
      PushdownTests.scala       # DataTypeTests, LiteralPushdownTests, OrderLimitTests, AggregateTests
      DialectProbeTests.scala   # Raw-SQL syntax probes per dialect flag
      DialectReport.scala       # Renders target/dialect-matrix.md
      Gaps.scala                # Reasons used in knownGaps
      Adbc{Postgres,Mysql,Mssql,Duckdb,Clickhouse,Trino,Datafusion}Test.scala  # Full suite per engine
      AdbcGizmosqlTest.scala    # Full suite over the Flight SQL driver (DuckDB server)
      AdbcSparkConnectTest.scala # Full suite against a Spark Connect server
      Adbc{Presto,Singlestore}Test.scala  # Full suites on pre-release drivers (dbc install --pre)
      AdbcExasolTest.scala      # Full suite; identifiers fold to upper case
      AdbcSqliteTest.scala      # Syntax probes only
      AdbcDriverPartitioningTest.scala
```

## Architecture

The connector follows the Spark DataSource V2 pattern with factory/builder layers:

- **Read path:** `DefaultSource` -> `AdbcTable` -> `AdbcScanBuilder` -> `AdbcScan` -> `AdbcBatch` -> `AdbcPartitionReaderFactory` -> `AdbcPartitionReader`
- **Write path:** `DefaultSource` -> `AdbcTable` -> `AdbcWriteBuilder` -> `AdbcWrite` -> `AdbcBatchWrite` -> `AdbcDataWriterFactory` -> `AdbcDataWriter`

Users configure the connector with options: `driver` (ADBC driver class), `uri` (database URI), `dialect` (SQL dialect), and either `dbtable` or `query`. Client-driven partitioning is supported via `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` options.

### SQL Dialect Support

`SqlDialect` is a set of choices a query generator has to make per engine. Five mirror existing or proposed SqlInfo codes (identifier quote 504, and limit syntax, null ordering syntax, boolean literal, date/time literal from apache/arrow#49796). Four more cover what the driver tests showed those don't: LIKE escaping (`ESCAPE '!'` clause vs implicit backslash), whether backslash is an escape inside string literals, whether non-ASCII literals need `N'...'`, and how an instant (timestamp with time zone) literal is written. Instants are always rendered in UTC with an explicit `+00:00`; a dialect with `InstantLiteral.Unsupported` keeps those comparisons in Spark.

Set via the `dialect` option:

- **`default`** — double-quoted identifiers, `LIMIT`, `NULLS FIRST/LAST`, `TRUE`/`FALSE`, `DATE '...'`/`TIMESTAMP '...'`, `LIKE ... ESCAPE '!'`, literal backslash, `TIMESTAMP WITH TIME ZONE '...'` for instants
- **`mssql`** — `OFFSET ... FETCH`, no `NULLS FIRST/LAST`, `1`/`0`, bare-string date/time and instant literals, `N'...'` for non-ASCII
- **`mysql`** — backtick identifiers, no `NULLS FIRST/LAST`, bare-string date/time and instant literals, backslash escapes
- **`clickhouse`** (also `chdb`) — bare-string date/time literals, implicit-backslash LIKE, backslash escapes, no instant literal
- **`datafusion`** — like default but implicit-backslash LIKE
- **`trino`** (also `presto`) — like default but `TIMESTAMP '...+00:00'` for instants
- **`singlestore`** — like mysql but with `NULLS FIRST/LAST`, implicit-backslash LIKE, no instant literal
- **`spark`** — backtick identifiers, backslash escapes, `TIMESTAMP '...+00:00'` for instants

When `dialect` is not set, the `jni.driver` name picks the dialect.

### Pushdown Support

The connector supports: column pruning, filter pushdown, limit pushdown, topN pushdown, and aggregate pushdown (COUNT, SUM, MIN, MAX, AVG). Aggregate output schemas are inferred by running the actual query shape against the database rather than hardcoding types.

### Client-Driven Partitioning

Range-based partitioning splits reads across N Spark partitions using a numeric column. Options: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` (all four required together). Follows the same stride-based approach as Spark's JDBC connector. When partitioning is active, aggregation/limit/topN pushdowns are disabled (Spark handles them after collecting all partitions).

### Driver-Driven Partitioning

Orthogonal to range partitioning: the `driverPartitioning` option (`auto` | `required` | `none`) makes `AdbcBatch.planInputPartitions` call `executePartitioned` for each generated query and flatten the returned descriptors into `AdbcDescriptorPartition`s (read on executors via `AdbcConnection.readPartition`). Plain queries become `AdbcQueryPartition`s. Defaults to `auto` without range options and `none` with them; `auto` falls back to plain queries on `NOT_IMPLEMENTED`. With it enabled, limit/topN are reported as partially pushed. The JNI driver doesn't implement it yet; `AdbcDriverPartitioningTest` uses a fake wrapping driver.

## Code Conventions

- 2-space indentation
- PascalCase for classes, camelCase for methods
- Package: `com.tokoko.spark.adbc`
- No formatter configured (no .scalafmt.conf)
- Tests extend `AnyFunSuite` with `BeforeAndAfterAll`

## Testing

- **Unit tests** (`src/test/`): Comet integration and JDBC benchmarks (require external PostgreSQL via Docker)
- **Driver tests** (`driver-tests/`): one suite per engine, all sharing the tables in `Fixtures.scala`. PostgreSQL, MySQL, MSSQL, ClickHouse, Trino, Presto, SingleStore, Exasol, GizmoSQL (DuckDB behind Flight SQL) and Spark Connect run in testcontainers (require Docker); DuckDB, DataFusion and SQLite are embedded. Native drivers come from `dbc install <name>` (`--pre` for presto and singlestore).
  - **Differential tests** (`checkSame`): the same DataFrame query runs through the connector and over an in-memory copy of the fixture; rows must match and the listed operators must really have been pushed down. Add a case by adding one line to a trait in `PushdownTests.scala`.
  - **Syntax probes** (`DialectProbeTests`): raw SQL variants for each dialect choice (limit/offset, NULLS FIRST/LAST, boolean and date/time literals, quoting, LIKE escaping, ...) sent straight through ADBC. The cross-engine result is written to `driver-tests/target/dialect-matrix.md`, together with the type mapping and known gaps.
  - **Known gaps**: a test that fails on an engine for an understood reason is listed in that suite's `knownGaps` and is cancelled instead of failed. If it starts passing, the suite fails until the entry is removed.
  - **Flight SQL**: the `flightsql` driver name says nothing about the server's SQL, so such a suite sets `dialectName`, which is passed as the `dialect` option.
  - **Upper-case folding engines** (Exasol): fixture columns are created quoted in lower case, so the suite sets `quoteProbeColumns` (probes name columns unquoted) and `ingestTable` (bulk ingest quotes the table name).
  - **Adding an engine**: extend `AdbcTestBase`, give `engine`, `adbcParams`, `sqlType` (native type per fixture type) and whatever of `setupLiteral`/`columnDdl`/`createTable` the engine needs.
  - The test JVM runs in a fixed non-UTC zone (`-Duser.timezone=Asia/Tbilisi`) so time zone mistakes in literals can't pass by accident.

Run driver tests: `pixi run sbt driverTests/test`
