# jdbc-shard-router

Applications that split a table across several databases end up routing each query by hand and silently return wrong totals or wrong pages when a query touches every shard. Middleware such as Vitess or ShardingSphere solves this but brings a proxy or a large framework. This library routes a documented SQL subset by shard key over plain JDBC DataSources and merges multi-shard results so they equal what one unsharded database returns.

## Installation

Add the dependency to your project:

```xml
<dependency>
  <groupId>io.github.trysac</groupId>
  <artifactId>jdbc-shard-router</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Usage

```java
ShardRouter router = ShardRouter.builder()
    .shards(List.of(shard0, shard1, shard2, shard3))
    .shardKey("orders", "customer_id")
    .build();
router.update(
    "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
    List.of(1, 42, new BigDecimal("19.95")));
ResultTable rows = router.query(
    "SELECT id, amount FROM orders WHERE customer_id = ?", List.of(42));
```

## Public API

- **`ShardRouter`**: The primary entry point. Configured via `Builder`. Methods: `query(String, List<Object>)` and `update(String, List<Object>)`.
- **`ResultTable`**: Immutable container for query results, holding `List<String> columns` and `List<List<Object>> rows`.
- **`RoutedEvent`**: Diagnostic record emitted on completion, containing `kind` ("query" or "update"), `shardCount`, and `durationNanos`.
- **`JumpHash`**: Implementation of Jump Consistent Hash for 64-bit keys (Lamping and Veach, 2014).
- **`ShardException`**: Runtime exception with stable error codes: `unsupported_sql`, `sql_too_long`, `missing_shard_key`, `scatter_write_forbidden`, `too_many_rows`, `shard_failure`, `invalid_parameter`.

### Limits and Options

- SQL length limit: 16,384 characters.
- Max merged rows: 100,000 (configurable via `maxRows`).
- SQL subset: `SELECT`, `INSERT`, `UPDATE`, `DELETE`. Joins, subqueries, `HAVING`, `DISTINCT` are not supported.

## Why not ShardingSphere?

ShardingSphere requires a proxy or a large framework; this library is a zero-runtime-dependency Java 21 library that routes a documented SQL subset by shard key over plain JDBC DataSources and merges multi-shard results so they equal what one unsharded database returns.

## Security

See [docs/SECURITY.md](docs/SECURITY.md).
