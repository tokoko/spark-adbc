# CLAUDE.md

## Project Overview

spark-adbc is a Spark DataSource V2 connector for Apache Arrow ADBC (Arrow Database Connectivity). It enables Spark SQL to read from and write to ADBC-compatible databases using Arrow's columnar format for efficient data transfer.

## Build System

This project uses **SBT** (Scala Build Tool) version 1.6.2.

- **Build:** `pixi run sbt compile`
- **Test:** `pixi run sbt test`
- **Package:** `pixi run sbt package`
- **Clean:** `pixi run sbt clean`

## Tech Stack

- **Language:** Scala 2.12.15
- **Framework:** Apache Spark 3.3.0 (DataSource V2 API)
- **Protocol:** Apache Arrow ADBC 0.1.0
- **Test framework:** ScalaTest (FunSuite style)
- **Test database:** Apache Derby (embedded)

## Project Structure

```
src/main/scala/
  com/tokoko/spark/adbc/     # Main connector implementation
    DefaultSource.scala       # Entry point (TableProvider)
    AdbcTable.scala           # Table (SupportsRead, SupportsWrite)
    AdbcScanBuilder.scala     # Read path: scan builder
    AdbcScan.scala            # Read path: scan
    AdbcBatch.scala           # Read path: batch/partition planning
    AdbcPartitionReaderFactory.scala
    AdbcPartitionReader.scala # Read path: executes ADBC query
    AdbcPartition.scala       # Partition descriptor
    AdbcWriteBuilder.scala    # Write path: write builder
    AdbcWrite.scala           # Write path: write
    AdbcBatchWrite.scala      # Write path: batch write
    AdbcDataWriterFactory.scala
    AdbcDataWriter.scala      # Write path: buffers rows, bulk inserts
    AdbcWriterCommitMessage.scala
  org/apache/spark/sql/util/
    ArrowUtilsExtended.scala  # Arrow <-> Spark format conversion utilities
src/test/scala/
  com/tokoko/spark/adbc/
    AdbcDerbyWriteReadTest.scala  # Integration test with embedded Derby
```

## Architecture

The connector follows the Spark DataSource V2 pattern with factory/builder layers:

- **Read path:** `DefaultSource` -> `AdbcTable` -> `AdbcScanBuilder` -> `AdbcScan` -> `AdbcBatch` -> `AdbcPartitionReaderFactory` -> `AdbcPartitionReader`
- **Write path:** `DefaultSource` -> `AdbcTable` -> `AdbcWriteBuilder` -> `AdbcWrite` -> `AdbcBatchWrite` -> `AdbcDataWriterFactory` -> `AdbcDataWriter`

Users configure the connector with options: `driver` (ADBC driver class), `url` (database URL), and either `dbtable` or `query`.

## Code Conventions

- 2-space indentation
- PascalCase for classes, camelCase for methods
- Package: `com.tokoko.spark.adbc`
- No formatter configured (no .scalafmt.conf)
- Tests extend `AnyFunSuite` with `BeforeAndAfterAll`

## Testing

Tests use an embedded Derby database to validate write-then-read round trips. The test database creates artifacts (`derbyDB/`, `metastore_db/`, `spark-warehouse/`) that are gitignored.

Run tests: `pixi run sbt test`
