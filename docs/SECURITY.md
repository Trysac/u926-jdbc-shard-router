# Security

## Threat Model and Safeguards

### Defended

- **SQL Injection**: The parser prohibits literals in `WHERE` and `SET` clauses. Every value must be bound via `?` placeholders, ensuring strictly parameterized queries.
- **Memory/Resource Bounds**: 
  - SQL inputs are capped at 16,384 characters (`sql_too_long`).
  - Merged query results are capped at `maxRows` (default 100,000), preventing heap exhaustion during merge operations (`too_many_rows`).
- **Leakage Prevention**: 
  - SQL statements and parameters are never logged.
  - Diagnostic `RoutedEvent` records contain only non-sensitive metrics (`kind`, `shardCount`, `durationNanos`).
  - Exception messages are sanitized; `shard_failure` messages contain only the shard index and SQLState, with message content truncated to 200 characters.

### Not Defended

- **Cross-shard Integrity**: This library executes statements per-shard. It does not provide global ACID transactions or two-phase commit across shards. If an `UPDATE` impacts multiple shards (though forbidden for keys), consistency is not guaranteed by this library.
- **Logic/Business Layer Attacks**: Applications must still perform their own authorization and business logic validation.
