# spark-adbc

A Spark DataSource V2 connector for [Apache Arrow ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity). It enables Spark to read from ADBC-compatible databases using Arrow's columnar format, avoiding the row-by-row serialization overhead of traditional JDBC.

## Features

- **Columnar data transfer** — reads data as Arrow columnar batches, enabling zero-copy integration with Spark's columnar execution engine
- **Read support** — read via `dbtable` or custom `query`
- **Predicate pushdown** — pushes filter expressions down to the database
- **Column pruning** — only fetches the columns Spark actually needs
- **Aggregate pushdown** — pushes `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and `GROUP BY` down to the database
- **Limit pushdown** — pushes `LIMIT` down to the database
- **Top-N pushdown** — pushes `ORDER BY ... LIMIT N` down to the database
- **Works with any ADBC driver** — PostgreSQL, SQLite, Flight SQL, Snowflake, DuckDB, etc.

> **Note:** Pushdown operations generate standard SQL syntax (e.g. `LIMIT`, `ORDER BY`). Some SQL dialects may not support all generated clauses, in which case those pushdowns will still work at the Spark level but won't be offloaded to the database.

## Usage

```scala
// Read from a table
val df = spark.read
  .format("com.tokoko.spark.adbc")
  .option("driver", "org.apache.arrow.adbc.driver.jni.JniDriverFactory")
  .option("jni.driver", "postgresql")
  .option("uri", "postgresql://user:pass@localhost:5432/mydb")
  .option("dbtable", "my_table")
  .load()

// Read from a query
val df = spark.read
  .format("com.tokoko.spark.adbc")
  .option("driver", "org.apache.arrow.adbc.driver.jni.JniDriverFactory")
  .option("jni.driver", "postgresql")
  .option("uri", "postgresql://user:pass@localhost:5432/mydb")
  .option("query", "SELECT id, name FROM my_table WHERE active = true")
  .load()
```

## Benchmarks

Local benchmarks comparing spark-adbc against Spark's built-in JDBC connector, reading **20 million rows** from PostgreSQL 16 (single-node, `local[*]` mode).

| Benchmark | ADBC | JDBC | Speedup |
|---|---|---|---|
| Full table scan | 73.9s | 135.8s | **1.84x** |
| Full table scan — columnar* | 38.3s | 142.6s | **3.72x** |
| Column projection (2 of 6 cols) | 34.2s | 64.3s | **1.88x** |
| Filtered read (`category = 'cat_5'`) | 5.6s | 6.2s | **1.11x** |
| Aggregation (`GROUP BY` with `AVG`, `SUM`, `COUNT`) | 4.1s | 49.2s | **11.91x** |

\*The columnar benchmark measures Arrow batch ingestion without Spark's columnar-to-row conversion. This represents a **theoretical upper bound** — Spark currently converts columnar batches back to rows for most operations, so real-world queries go through the standard (non-columnar) read path. Native columnar execution engines like [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) can unlock this potential by operating directly on Arrow batches.
