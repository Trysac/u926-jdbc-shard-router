package io.github.trysac.jdbc_shard_router;

import java.util.Objects;

/**
 * Diagnostic event emitted after a query or update completes.
 *
 * <p>Never contains SQL text or parameter values.
 *
 * @param kind {@code "query"} or {@code "update"}
 * @param shardCount the number of shards contacted (1 for point-routed statements, or total shards
 *     for scatter-gather queries)
 * @param durationNanos elapsed execution time in nanoseconds
 */
public record RoutedEvent(String kind, int shardCount, long durationNanos) {

  /** Validates the event fields. */
  public RoutedEvent {
    Objects.requireNonNull(kind, "kind");
    if (!kind.equals("query") && !kind.equals("update")) {
      throw new IllegalArgumentException("kind must be 'query' or 'update'");
    }
    if (shardCount < 1) {
      throw new IllegalArgumentException("shardCount must be >= 1");
    }
    if (durationNanos < 0) {
      throw new IllegalArgumentException("durationNanos must be >= 0");
    }
  }
}
