# SPIP: ADBC Data Source for Apache Spark

## Q1. What are you proposing?

A new first-party DataSource V2 connector in Apache Spark that uses Apache Arrow ADBC (Arrow Database Connectivity) — an API specification for database drivers that exposes query results as Arrow record batches — to read from and write to relational databases. ADBC is analogous to JDBC in that it is a client-side API standard; the underlying wire protocol varies per driver (Postgres wire protocol, Arrow Flight SQL, Snowflake's HTTP API, etc.). The connector would live alongside the existing JDBC data source (`spark.read.format("jdbc")`) as a higher-performance alternative for databases that ship ADBC drivers, targeting users today exposed to row-oriented JDBC serialization overhead on analytic workloads.

## Q2. What problem is this proposal trying to solve?

Spark's JDBC data source serializes every row through the JDBC `ResultSet` abstraction — one row at a time, one Java object per cell, with per-cell `ResultSet.getXxx` dispatch and type coercion. For analytic reads pulling tens of millions of rows from a database whose native protocol is columnar (Arrow Flight SQL, Snowflake's result format, BigQuery's Storage Read API, Postgres `COPY BINARY`, etc.), this row-at-a-time path dominates wall-clock time. Benchmarks on the existing `spark-adbc` prototype show 3–10× throughput improvements over JDBC on columnar-backend reads, with the margin widening on wider tables.

Two distinct costs contribute:

1. **Driver-boundary shredding.** Columnar-protocol backends have to decode their columnar payload into rows to fit the JDBC `ResultSet` interface. Work that was columnar upstream gets pointlessly row-ified at the driver.
2. **Per-cell dispatch in Spark.** Even once data is row-based, building Spark's `InternalRow` via per-column `ResultSet.getXxx` calls, Java object boxing for nullable types, and type coercion is expensive relative to a bulk Arrow-to-Spark conversion of a whole batch.

ADBC addresses both by delivering results as Arrow `RecordBatch` streams. The data source consumes batches directly:

- For **Spark's row-based execution path**, Arrow batches convert to `InternalRow` in bulk (vectorized) without per-cell JDBC-style dispatch.
- For **Spark's columnar scan path** (`PartitionReader[ColumnarBatch]`, used by vectorized execution engines like Comet and Gluten), the Arrow batch is wrapped as a `ColumnarBatch` with near-zero copy — the advantage compounds because downstream operators stay columnar.

The current prototype uses the columnar path; both are viable and users with and without vectorized engines benefit.

## Q3. How is it done today, and what are the limits of current practice?

**Today — Spark's stock JDBC connector:**
- Row-oriented `ResultSet` → `InternalRow` conversion per row
- One JDBC `PreparedStatement` per partition, row-batched `INSERT` writes
- Rich pushdown support (filters, limit, topN, aggregates, columns) via `JdbcDialect` subclasses
- Broad driver ecosystem but uniformly serializes through rows

**Limits:**
- JDBC has no columnar API, so columnar-capable backends lose throughput at the driver boundary
- Writes are row-batched `INSERT`s; no access to native bulk-load paths (Postgres `COPY`, Snowflake stage+COPY, BigQuery load jobs) without per-dialect escape hatches
- Type coercion happens via `java.sql.Types`, losing fidelity for Arrow-native types (decimals, timestamps with timezones, nested types, lists)

**Related third-party work:**
- `tokoko/spark-adbc` — the prototype this SPIP is based on; demonstrates the approach works
- Snowflake, Databricks, BigQuery Spark connectors — each vendor-specific, duplicating infrastructure that ADBC already standardizes
- Spark Arrow integration — Spark internally uses Arrow for some operations (Python UDFs, pandas API) but not at the data-source boundary

## Q4. What is new in your approach and why do you think it will be successful?

### 4.1 Read path

The read path maps directly to ADBC's columnar streaming model:

1. `DefaultSource.inferSchema` opens an ADBC connection and calls `AdbcStatement.executeSchema(SELECT * FROM <relation>)` to retrieve the Arrow schema without executing the query. Falls back to `executeQuery` on `SELECT ... WHERE 1=0` for drivers that don't implement `executeSchema`.
2. `AdbcScanBuilder` accepts Spark pushdowns (`pruneColumns`, `pushFilters`, `pushLimit`, `pushTopN`, `pushAggregation`) and compiles them into SQL. Aggregate expressions are emitted with explicit aliases (`COUNT(*) AS agg_0`) so derived-table wrapping works uniformly across dialects.
3. At planning time, the connector decides between:
   - **Server-driven partitioning** via `AdbcStatement.executePartitions` — driver hands back opaque partition descriptors, one per Spark task. Used when supported (Flight SQL backends today; broader adoption expected).
   - **Client-driven partitioning** — stride-based split on a numeric column, mirroring the JDBC connector's `partitionColumn` / `lowerBound` / `upperBound` / `numPartitions` options. Generates N parallel `SELECT ... WHERE col BETWEEN ...` queries.
   - **Single-partition** — default for small tables.
4. `AdbcPartitionReader` executes its query and consumes the Arrow record batch stream. It exposes itself as a `PartitionReader[ColumnarBatch]`, wrapping each Arrow batch as a Spark `ColumnarBatch` via `ArrowColumnVector`. Spark's row-based operators consume this via the DSv2 columnar-to-row adapter (no per-cell JDBC-style dispatch); vectorized engines like Comet and Gluten consume it directly.

**Why this succeeds:**
- Columnar data at the driver boundary eliminates row-shredding for columnar-protocol backends
- Arrow-to-Spark conversion happens in bulk per batch rather than per-cell via `getXxx` dispatch
- Pushdown surface matches JDBC's — no feature regression for users switching over
- Arrow schema preserves type fidelity (decimals with precision/scale, lists, structs, timestamps with timezones) that JDBC flattens through `java.sql.Types`

### 4.2 Write path

This SPIP intentionally restricts the initial write path to **single-node writes** via ADBC's `bulkIngest` API, with per-task transactions for atomicity:

- Spark's `DataWriter.write` buffers Arrow batches in memory
- `DataWriter.commit` opens an ADBC connection, calls `bulkIngest(table, APPEND)` with the buffered batches, commits the per-task transaction
- `DataWriter.abort` rolls back and closes resources
- `BatchWrite.commit/abort` are no-ops — each task is atomic, but there is **no cross-task atomicity**; partial writes remain if the job fails after some tasks succeeded

This matches the semantics of Spark's stock JDBC writer (at-least-once append). It's a deliberate scoping choice. Proper distributed write coordination (staging + merge, 2PC, etc.) requires either:

1. Significant application-level complexity inside the connector (staging-table creation requires DDL privileges many users don't have)
2. A symmetric "partitioned write" API in the ADBC spec — **does not exist today**. No RFC, issue, or mailing-list thread proposes one as of 2026-04. When it lands, the connector can switch to server-driven distributed writes analogous to server-driven partitioned reads.

Pushing the distributed-write problem down to the ADBC spec — where any driver implementation can solve it uniformly for every client — is a better use of effort than re-implementing staging logic per-dialect in the Spark connector. We recommend tracking a follow-up SPIP once ADBC gains partitioned-write primitives.

### 4.3 SQL dialect handling

JDBC's `JdbcDialect` is the de facto reference for dialect abstraction in Spark. The ADBC data source should follow the same pattern but ideally source dialect metadata *from the ADBC driver itself* where possible.

**Proposal:** a small declarative `SqlDialect` case class with flags covering the pushdown-relevant axes:

```
identifierQuote          : DoubleQuote | Backtick | Bracket
limitSyntax              : LimitN | TopN | FetchFirst | None
nullsOrderingSyntax      : NullsFirstLast | Unsupported
parameterStyle           : QuestionMark | DollarPositional | NamedColon | NamedAt
supportedAggregates      : bitmask { COUNT, SUM, AVG, MIN, MAX, ... }
```

Resolution order at connect time:

1. If the driver populates relevant Flight SQL `SqlInfo` codes (504 `SQL_IDENTIFIER_QUOTE_CHAR`, 507 `SQL_NULL_ORDERING`, 522 `SQL_SUPPORTED_GROUP_BY`, etc.) via `AdbcConnection.getInfo`, use those values directly.
2. Otherwise, look up a built-in preset keyed off `ADBC_INFO_VENDOR_NAME` (code 0), which every driver does populate — similar to `JdbcDialects.get(url)`.
3. User-provided `dialect=<name>` option overrides both.

**Status of driver support today (measured, ADBC 0.23.0):**

| Driver | Populates SqlInfo dialect codes? |
|---|---|
| Postgres (Apache) | No |
| DuckDB (DuckDB Foundation) | No |
| MySQL (Foundry) | No — returns codes with null values |
| MSSQL (Columnar) | No — returns codes with null values |

Every driver populates `VENDOR_NAME` (code 0), so the vendor-keyed preset path works universally today. The `SqlInfo`-driven path is a forward-compatibility mechanism: as drivers implement the spec more completely, Spark automatically picks up dialect information without connector changes.

**This matters because:** the Spark JDBC connector's dialect layer is per-subclass, procedural, and additions require Spark changes. The ADBC approach pushes as much dialect knowledge as possible into the driver — where it belongs — leaving Spark with a generic fallback.

## Q5. Who cares? If you are successful, what difference will it make?

- **Analytical Spark users** pulling large result sets from Postgres/DuckDB/Snowflake/BigQuery/Flight SQL backends gain substantial read throughput without changing query patterns
- **Driver vendors** gain a first-class Spark integration path without maintaining their own connector (Snowflake, Databricks, BigQuery all ship Spark connectors today — each duplicates machinery that ADBC standardizes)
- **Spark contributors** get a cleaner dialect abstraction that can evolve with the ADBC spec rather than accumulating per-vendor `JdbcDialect` subclasses
- **Arrow ecosystem** gains a canonical example of ADBC integration in a widely-used downstream project, accelerating adoption pressure on driver implementations

## Q6. What are the risks?

- **ADBC driver ecosystem maturity** — Postgres, SQLite, DuckDB, Snowflake, Flight SQL, MySQL, BigQuery all ship drivers, but they implement the spec unevenly (`executeSchema`, `getStatistics`, `getInfo` codes). The connector must degrade gracefully; we've validated fallback paths for the measured gaps.
- **Native dependencies** — ADBC drivers are typically native (C/C++/Go); the JNI driver introduces platform-specific packaging concerns that pure-Java JDBC drivers don't have. Mitigation: make ADBC an optional data source; users opt in.
- **Type mapping edge cases** — Arrow-to-Spark type mapping already exists (used by pandas UDFs), but some Arrow types (extension types, large unions) have no Spark equivalent and need defined fallbacks.
- **Write semantics expectations** — users moving from other connectors may expect cross-task atomicity. Mitigation: clearly document at-least-once semantics; defer stronger guarantees to the ADBC-spec level.
- **Spec evolution** — ADBC is still in pre-2.0; spec changes could affect the connector. Mitigation: pin to a spec version in the initial release; treat upgrades as Spark-version-scoped follow-ups.

## Q7. How long will it take?

Based on the `tokoko/spark-adbc` prototype (working, 76 integration tests passing across DuckDB/MySQL/Postgres/MSSQL):

- **1–2 months**: productionize the read path, port the dialect layer, integrate with Spark's catalog/table APIs, documentation
- **1 month**: single-node write path with proper resource management, transaction handling, and tests
- **1–2 months**: Spark PR review cycles, addressing feedback, getting into a Spark release

Total: ~3–5 months from SPIP acceptance to merge.

## Q8. Mid-term and final "exams"

**Mid-term checkpoints:**
- All JDBC-data-source integration tests pass against an equivalent ADBC configuration (Postgres minimum) — demonstrates feature parity
- Read benchmark: 3× throughput improvement over JDBC on TPC-H lineitem-scale tables, measured on at least two columnar backends

**Final exam:**
- Merged into Spark master as an optional data source
- At least three driver backends covered by CI (Postgres, DuckDB or Flight SQL, one more)
- Documentation: user guide, dialect-authoring guide, ADBC-driver-author checklist for maximizing pushdown support

---

## Appendix A: API surface

No public Spark API changes. Users opt in via:

```scala
spark.read
  .format("adbc")
  .option("driver", "<driver class or JNI driver name>")
  .option("uri", "postgresql://...")
  .option("dbtable", "orders")  // or .option("query", "SELECT ...")
  .load()
```

Configuration options mirror the JDBC data source where meaningful (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`) and add ADBC-specific ones (`driver`, `uri`, optional `dialect`).

## Appendix B: Related work and prototype

- Working prototype: https://github.com/tokoko/spark-adbc — 76 integration tests across 4 databases
- Apache Arrow ADBC spec: https://arrow.apache.org/adbc/
- Spark DataSource V2 API: `org.apache.spark.sql.connector.*`
- Precedent for data-source additions via SPIP: data-source-v2 itself, Kafka source, etc.
