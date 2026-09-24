# SPIP: ADBC Data Source for Apache Spark

## Q1. What are you trying to do?

Spark connects to most databases through JDBC, a standard Java interface for database drivers. JDBC passes data one row at a time, and Spark reads each value in a row separately. Many modern analytical databases store and send data in columns. So when Spark reads from them through JDBC, the data gets converted from columns to rows and then read value by value. On large reads, that conversion takes up much of the total time.

ADBC (Arrow Database Connectivity) is a newer standard for database drivers from the Apache Arrow project. ADBC drivers return results as batches of columns in Arrow format. Spark already uses Arrow for its Python integration.

We propose adding a built-in ADBC data source to Spark, next to the existing JDBC one. It would read and write through any ADBC driver and pass the column batches straight into Spark. This would make large analytical reads faster without changing how users write queries.

## Q2. What problem is this proposal NOT designed to solve?

- **Replacing JDBC.** The ADBC data source complements the JDBC data source; it doesn't replace it. JDBC supports far more databases and has a mature ecosystem, and it stays unchanged: no deprecation and no behavior changes. ADBC gives users a second option for databases that ship ADBC drivers. The gains are largest on analytical reads from databases that already store or send data in columns.
- **Atomic distributed writes in the first version.** The first version writes from each task independently, like Spark's JDBC writer does today. It doesn't guarantee that a whole job's writes succeed or fail together. That needs the partitioned bulk ingest API proposed in ADBC ([apache/arrow-adbc#4317](https://github.com/apache/arrow-adbc/pull/4317)), and will follow once it is merged.
- **Structured Streaming.** The proposal covers batch reads and writes only.

## Q3. How is it done today, and what are the limits of current practice?

**Today.** Spark's JDBC data source is the general way to read from and write to databases. It supports many databases, and it can push column selection, filters, limits, Top-N and aggregates down to the database through `JdbcDialect` subclasses. Every read goes through the row-by-row JDBC `ResultSet` interface, and writes are batched `INSERT` statements.

**Limits.**

1. **Converting column data to rows at the driver.** JDBC has no way to return data in columns. So drivers for databases that send data in columns (Arrow Flight SQL, Snowflake, BigQuery's Storage Read API) must convert it to rows to fit `ResultSet`. That undoes work the database already did.
2. **Reading each value separately in Spark.** Even with row data, Spark builds each `InternalRow` with one `ResultSet.getXxx` call per value. Each call also boxes nullable values as Java objects and converts types. That costs much more than converting a whole Arrow batch at once.
3. **Converting to columns again downstream.** Vectorized engines (Comet, Gluten) then have to convert the rows back into columns. ADBC batches can be wrapped as a Spark `ColumnarBatch` almost without copying, so data can stay in columns end to end.
4. **Type precision.** Types are mapped through `java.sql.Types`, which loses detail for decimals, timestamps with time zones, lists and structs.
5. **Writes.** Writes are batched `INSERT` statements, with no general way to use a database's own bulk-load path.

Benchmarks on the `spark-adbc` prototype show 3–10× read throughput over JDBC on column-oriented databases, and the gap grows on wider tables.

**Related work.**
- `tokoko/spark-adbc`: the working prototype this SPIP is based on.
- Vendor Spark connectors (Snowflake, BigQuery, Databricks): each one rebuilds, for a single vendor, what ADBC already standardizes.

## Q4. What is new in your approach and why do you think it will be successful?

**What's new**

1. **Data stays in columns from the database to Spark.** Each task reads Arrow batches from the ADBC driver and wraps them as Spark `ColumnarBatch`es through `ArrowColumnVector`, without copying. Spark's row-based operators read them through the standard columnar-to-row step. Vectorized engines (Comet, Gluten) use them directly.
2. **The database can split the work.** Besides JDBC-style range partitioning on a column, the data source can ask the ADBC driver to split a query's result (`executePartitioned`). Each piece becomes one Spark task. Flight SQL servers already return results this way, and JDBC has no equivalent.
3. **One connector for many databases.** Any database with an ADBC driver works through the same code. Spark keeps no per-database dialect code: the driver reports the SQL syntax its database uses.
4. **Arrow types.** Schemas come from the driver in Arrow format, which keeps decimal precision and scale, timestamps with time zones, lists and structs.

**Why we think it will succeed**

- **Working prototype.** It pushes down column selection, filters, limit, Top-N and COUNT/SUM/MIN/MAX/AVG aggregates. It supports range and driver partitioning plus appending writes, and it is tested against several databases.
- **Low risk to Spark.** It's a new optional data source: existing APIs and the JDBC source are unchanged. It reuses Spark's existing Arrow conversion code (`ArrowColumnVector`, `ArrowWriter`, `ArrowUtils`).
- **Improvements are shared.** Apache Arrow maintains ADBC, and drivers exist for Postgres, SQLite, DuckDB, Snowflake, BigQuery, Flight SQL, MySQL and MSSQL. When a driver improves, Spark benefits without connector changes.
- **Limited scope.** Writes are appends that can repeat rows if a job is retried, the same guarantee as the JDBC writer. Atomic writes across tasks wait for ADBC's partitioned bulk ingest API (see Q2).

## Q5. Who cares? If you are successful, what difference will it make?

- **Analytical Spark users** pulling large result sets from Postgres/DuckDB/Snowflake/BigQuery/Flight SQL backends gain substantial read throughput without changing query patterns
- **Driver vendors** gain a first-class Spark integration path without maintaining their own connector (Snowflake, Databricks, BigQuery all ship Spark connectors today — each duplicates machinery that ADBC standardizes)
- **Spark contributors** don't maintain per-database dialect code: drivers report their SQL syntax, instead of Spark accumulating `JdbcDialect` subclasses
- **Arrow ecosystem** gains a canonical example of ADBC integration in a widely-used downstream project, accelerating adoption pressure on driver implementations

## Q6. What are the risks?

- **ADBC driver ecosystem maturity** — Postgres, SQLite, DuckDB, Snowflake, Flight SQL, MySQL and BigQuery all ship drivers, but they implement the spec unevenly. Mitigation: the connector falls back when an optional feature is missing: `executeSchema` → `WHERE 1=0` query, `executePartitioned` → plain queries, `setAutoCommit(false)` → writing without a transaction.
- **Native dependencies** — ADBC drivers are typically native (C/C++/Go); the JNI driver introduces platform-specific packaging concerns that pure-Java JDBC drivers don't have. Mitigation: make ADBC an optional data source; users opt in.
- **Spec evolution** — the ADBC spec (1.1) is versioned and backward compatible, but the Java libraries are still 0.x and their APIs can change. Mitigation: pin library versions per Spark release and treat upgrades as follow-ups.
- **Upstream prerequisites stalling** — atomic writes depend on ADBC's partitioned bulk ingest API ([apache/arrow-adbc#4317](https://github.com/apache/arrow-adbc/pull/4317)), and database-specific SQL syntax depends on new Flight SQL `SqlInfo` codes ([apache/arrow#49796](https://github.com/apache/arrow/pull/49796)). Both are still open. Mitigation: neither blocks the first version, which uses per-task writes and ANSI SQL with user overrides.
- **Type mapping edge cases** — Arrow-to-Spark type mapping already exists (used by pandas UDFs), but some Arrow types (extension types, large unions) have no Spark equivalent and need defined fallbacks.
- **Uneven driver support for partitioned execution** — as of ADBC 0.23.0 the JNI bridge (`adbc-driver-jni`) doesn't implement `executePartitioned` or `readPartition`, so native drivers loaded through it (including the Go Flight SQL driver) always use plain queries. The pure-Java Flight SQL driver supports it. Mitigation: `auto` falls back; closing the JNI gap is an upstream ADBC contribution.

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

Configuration options mirror the JDBC data source where meaningful (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`) and add ADBC-specific ones (`driver`, `uri`, optional dialect overrides, optional `driverPartitioning` = `auto` | `required` | `none`).

## Appendix B: Design sketch

### B.1 Read path

1. **Schema.** `inferSchema` calls `AdbcStatement.executeSchema` on `SELECT * FROM <relation>`. If the driver doesn't implement it, it runs the query with `WHERE 1=0` instead.
2. **Pushdown.** `AdbcScanBuilder` turns column pruning, filters, limit, Top-N and aggregates into SQL. Aggregates get explicit aliases (`COUNT(*) AS agg_0`) so the query can be wrapped as a subquery in any dialect. OFFSET, TABLESAMPLE and more aggregate functions (which JDBC supports) can be added later.
3. **Partitioning.** There are two independent mechanisms, and they can be combined:
   - **Range partitioning** decides which queries run. It works like JDBC's `partitionColumn` / `lowerBound` / `upperBound` / `numPartitions` options and produces N range queries.
   - **Driver partitioning** (`driverPartitioning` = `auto` | `required` | `none`) decides how each query's result is read. The Spark driver calls `executePartitioned`, and each returned descriptor becomes one Spark task that reads it with `readPartition`. With `auto`, drivers that return `NOT_IMPLEMENTED` fall back to plain queries.

   Combining them gives N×M partitions, which guarantees some parallelism when a server returns few partitions. Driver partitioning defaults to `auto` without range options and to `none` with them.

   Descriptors are produced in `Batch.planInputPartitions()`, not `ScanBuilder.build()`, so `explain()` and schema analysis don't run queries. The result is cached, so each scan runs its queries once. The first range query is a probe, and the rest run concurrently.

   Range partitioning disables limit, Top-N and aggregate pushdown. Driver partitions are slices of a single result, so aggregates are still pushed. Limit and Top-N are pushed but marked partially pushed, because order across descriptors isn't guaranteed.
4. **Reading.** `AdbcPartitionReader` runs its query or reads its descriptor. It wraps each Arrow batch as a `ColumnarBatch` through `ArrowColumnVector`. Row-based operators read it through Spark's columnar-to-row step, and Comet and Gluten read it directly.

### B.2 Write path

**First version: independent per-task writes.** Each Spark task opens its own connection and writes its partition through ADBC's `bulkIngest(table, APPEND)`. When the driver supports transactions, each task writes in its own transaction: it's committed in `DataWriter.commit` and rolled back in `abort`. Drivers that return `NOT_IMPLEMENTED` for `setAutoCommit(false)` write without a transaction. Tasks aren't coordinated, so `BatchWrite.commit` / `abort` do nothing. If a job fails after some tasks have committed, their rows stay in the table. This is the same at-least-once append behavior as Spark's JDBC writer.

**Later: coordinated writes.** The partitioned bulk ingest API proposed in [apache/arrow-adbc#4317](https://github.com/apache/arrow-adbc/pull/4317) mirrors `executePartitioned` / `readPartition` for writes, and maps directly onto Spark's write commit protocol:

- `ConnectionBulkIngestInit`, on the Spark driver when the `BatchWrite` is created: checks the target table and schema once.
- `ConnectionInsertPartition`, in each `DataWriter`: stages that task's data and returns a serializable handle, which is sent back in the `WriterCommitMessage`.
- `ConnectionCompleteIngestPartitions`, in `BatchWrite.commit`: commits all staged handles together.

Once that API is merged and drivers implement it, the connector will use it in place of independent per-task writes, making the whole job atomic. Until then, we avoid building staging tables for each database inside Spark, which would also need DDL privileges many users don't have.

### B.3 SQL dialect handling

**Goal: no dialect definitions in Spark.** Spark's JDBC source needs a `JdbcDialect` subclass per database, and adding a database means changing Spark. The ADBC source instead asks the driver how to write SQL for its database, through `AdbcConnection.getInfo` and Flight SQL `SqlInfo` codes. The connector only needs to know the few syntax choices that pushdown depends on:

| Syntax choice | `SqlInfo` code |
|---|---|
| Identifier quote character | 504 `SQL_IDENTIFIER_QUOTE_CHAR` (exists today) |
| LIMIT / OFFSET syntax | 577 `SQL_SUPPORTED_LIMIT_OFFSET` |
| `NULLS FIRST` / `NULLS LAST` support | 578 `SQL_SUPPORTED_NULLS_ORDERING` |
| Boolean literals | 579 `SQL_SUPPORTED_BOOLEAN_LITERAL` |
| Date/time literals | 580 `SQL_SUPPORTED_DATETIME_LITERAL` |

Codes 577–580 are proposed in [apache/arrow#49796](https://github.com/apache/arrow/pull/49796). That PR is a soft prerequisite: the connector works without it, but can only use a database's own syntax once drivers report these codes.

**Fallback.** When a driver doesn't report a code, the connector uses ANSI SQL for it, and users can override each value with a data source option. No driver reports these codes today (measured with Postgres, DuckDB, MySQL and MSSQL drivers on ADBC 0.23.0), so the fallback is what users get at first. The prototype currently uses built-in presets (ANSI, MSSQL, MySQL) as a stopgap. They will be removed in favor of driver-reported values and per-option overrides.

## References

- Working prototype: https://github.com/tokoko/spark-adbc — 76 integration tests across 4 databases
- Apache Arrow ADBC spec: https://arrow.apache.org/adbc/
- Spark DataSource V2 API: `org.apache.spark.sql.connector.*`
- Precedent for data-source additions via SPIP: data-source-v2 itself, Kafka source, etc.
