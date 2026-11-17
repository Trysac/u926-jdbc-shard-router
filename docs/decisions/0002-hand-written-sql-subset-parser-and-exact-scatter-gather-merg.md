# Hand-Written SQL Subset Parser and Exact Scatter-Gather Merge Engine

Status: accepted (generated, pending human review)

## Context

Applications sharding tables across multiple JDBC DataSources need consistent routing and accurate scatter-gather query results for ORDER BY, LIMIT/OFFSET, GROUP BY, and aggregates without deploying external database proxies or heavyweight frameworks.

## Decision

Use Jump Consistent Hashing (with 64-bit FNV-1a for String keys) for shard assignment, parse the supported SQL subset with a zero-dependency hand-written parser that enforces '?' parameterization and a 16,384-character limit, and merge scatter-gather results using k-way heap merge for ORDER BY (pushing down LIMIT n+m) and SUM/COUNT rewriting for AVG with scale 10 and HALF_EVEN rounding.

## Consequences

The library has zero runtime dependencies outside the JDK, guarantees that multi-shard queries match an unsharded reference database on the supported SQL subset, prevents accidental scatter writes, and avoids leaking SQL text or parameter values in events or exception messages.
