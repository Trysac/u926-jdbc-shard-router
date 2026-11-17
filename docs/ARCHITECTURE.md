# Architecture

`jdbc-shard-router` is a zero-runtime-dependency Java 21 library (`java.sql` and `javax.sql.DataSource`)
that routes a deterministic SQL subset across JDBC shards and merges scatter-gather results.

## Components

1. **`JumpHash`**: Implements Lamping and Veach (2014) Jump Consistent Hash for 64-bit integer keys,
   and hashes `String` keys with 64-bit FNV-1a over UTF-8 bytes before bucketing.
2. **`SqlParser` & `SqlAst`**: Hand-written lexer and recursive-descent parser enforcing a maximum
   SQL length of 16,384 characters (`sql_too_long`) and rejecting unsupported constructs (`JOIN`,
   subqueries, `HAVING`, `DISTINCT`, `OR`, inline literals in `WHERE`, etc.) with `unsupported_sql`.
3. **`ShardRouter`**:
   - **Routing**: `SELECT`, `UPDATE`, and `DELETE` statements containing `WHERE <shard_key> = ?`
     route to `JumpHash.bucket(key, shards.size())`. `INSERT` extracts the shard key column value.
     `UPDATE` and `DELETE` without the shard key equality predicate fail with
     `scatter_write_forbidden`.
   - **Scatter-Gather Execution**: Queries without a shard key equality predicate execute in
     parallel on a bounded thread pool sized to `shards.size()`.
   - **Result Merging**:
     - `ORDER BY`: Pushes `ORDER BY` and `LIMIT (n + m)` to each shard and performs a k-way heap
       merge before applying `OFFSET m` and `LIMIT n`.
     - Aggregates: `COUNT` and `SUM` add across shards; `MIN` and `MAX` compare; `AVG(c)` is
       rewritten into `SUM(c), COUNT(c)` per shard and merged as `SUM / COUNT` with scale 10 and
       `RoundingMode.HALF_EVEN`.
     - `GROUP BY`: Groups rows across shards by key, folds aggregates per group, then applies
       `ORDER BY` and `LIMIT / OFFSET`.
