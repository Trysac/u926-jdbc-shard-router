# Project record

## Objective

JDBC shard router that routes statements by shard key with jump consistent hashing and merges scatter-gather results for ORDER BY, LIMIT, GROUP BY and aggregates.

## Scope

- The public API documented in [README.md](../README.md).
- Deterministic SQL parsing and execution for supported statements.
- Consistent hashing of shard keys across configured shards.
- Exact merge of multi-shard query results (ORDER BY, GROUP BY, aggregates).

## Out of Scope

- Joins and subqueries.
- HAVING and DISTINCT clauses.
- Cross-shard transactions or two-phase commit.
- Resharding or automated data migration.
- JDBC Driver or DataSource facade implementation.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created. |

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 3233408 / 34859 |
| Hours (human) | pending measurement |
| Cost (USD) | 1.2128 |

## Factory run u926 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-17 | generation | google/gemini-4-argon | yes | 7464 | 27146 | 0 | 0 | 0.5728 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 139615 | 1555 | 0 | 722580 | 0.1647 |
| 2026-11-17 | fix-3 | google/gemini-3.8-flash | no | 132995 | 1929 | 0 | 869530 | 0.1722 |
| 2026-11-17 | fix-4 | google/gemini-3.8-flash | no | 145996 | 1223 | 0 | 731053 | 0.1689 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 127718 | 1359 | 0 | 333135 | 0.1259 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 23322 | 1647 | 0 | 0 | 0.0083 |

Total: $1.2128. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
