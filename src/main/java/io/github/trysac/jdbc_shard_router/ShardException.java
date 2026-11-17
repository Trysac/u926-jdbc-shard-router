package io.github.trysac.jdbc_shard_router;

import java.util.Objects;

/**
 * Exception thrown by {@link ShardRouter} when routing, parsing, executing, or merging fails.
 *
 * <p>Error codes:
 *
 * <ul>
 *   <li>{@code unsupported_sql} - statement is invalid or uses unsupported SQL constructs
 *   <li>{@code sql_too_long} - statement exceeds the 16,384 character limit
 *   <li>{@code missing_shard_key} - table has no registered shard key or INSERT omits it
 *   <li>{@code scatter_write_forbidden} - UPDATE or DELETE does not constrain the shard key with
 *       {@code = ?}
 *   <li>{@code too_many_rows} - merged result exceeds {@code maxRows}
 *   <li>{@code shard_failure} - a shard threw an {@link java.sql.SQLException}
 *   <li>{@code invalid_parameter} - parameter list or shard key value is invalid
 * </ul>
 */
public final class ShardException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String code;
  private final int shardIndex;

  /**
   * Creates a non-shard-specific exception.
   *
   * @param code the stable machine-readable error code
   * @param message the error message (never containing secrets or parameter values)
   */
  public ShardException(String code, String message) {
    this(code, message, -1, null);
  }

  /**
   * Creates a shard-specific exception.
   *
   * @param code the stable machine-readable error code
   * @param message the error message (never containing secrets or parameter values)
   * @param shardIndex the zero-based shard index, or -1 if not shard-specific
   * @param cause the underlying cause, or {@code null}
   */
  public ShardException(String code, String message, int shardIndex, Throwable cause) {
    super(sanitizeMessage(message), cause);
    this.code = Objects.requireNonNull(code, "code");
    this.shardIndex = shardIndex;
  }

  private static String sanitizeMessage(String message) {
    if (message == null) {
      return "";
    }
    if (message.length() > 200) {
      return message.substring(0, 200);
    }
    return message;
  }

  /**
   * Returns the machine-readable error code.
   *
   * @return the error code
   */
  public String code() {
    return code;
  }

  /**
   * Returns the zero-based index of the shard that failed, or {@code -1} when not shard-specific.
   *
   * @return the shard index or {@code -1}
   */
  public int shardIndex() {
    return shardIndex;
  }
}
