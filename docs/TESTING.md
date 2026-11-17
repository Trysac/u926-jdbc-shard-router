# Testing

## Running

```sh
mvn test
```

## Tests

- **`ShardRouterTest.java`**: 
    - **JumpHash Distribution/Monotonicity**: Property tests using jqwik verifying key distribution (1,500–2,500 keys moved) and monotonicity when scaling from 4 to 5 buckets.
    - **Differential Testing**: Compares router output against a single, unsharded H2 reference database using 1,000 random operations (insertions and queries) across 12+ scenarios (point lookups, range scans, aggregates, GROUP BY, ORDER BY, LIMIT/OFFSET).
    - **Error Handling**: Verifies that invalid SQL or operational failures throw expected `ShardException` codes (`unsupported_sql`, `too_many_rows`, `shard_failure`, etc.).
    - **Aggregation Logic**: Specifically validates that `AVG` is correctly rewritten as `SUM` and `COUNT` with consistent scale and rounding.

## Policy

- All tests are deterministic: no wall clock dependency, no network I/O.
- Every public API change or bug fix requires a corresponding unit or differential test case.
- Test coverage is maintained via JaCoCo to ensure all merge and routing paths are exercised.
