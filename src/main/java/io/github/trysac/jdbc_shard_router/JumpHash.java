package io.github.trysac.jdbc_shard_router;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Jump Consistent Hash implementation (Lamping and Veach, 2014).
 *
 * <p>String keys are hashed using 64-bit FNV-1a over UTF-8 bytes before bucketing.
 */
public final class JumpHash {

  private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long FNV_64_PRIME = 0x100000001b3L;
  private static final long JUMP_MULTIPLIER = 2862933555777941757L;

  private JumpHash() {}

  /**
   * Assigns a 64-bit key to a bucket in {@code [0, buckets)}.
   *
   * @param key the 64-bit key
   * @param buckets the number of buckets (must be positive)
   * @return the bucket index in {@code [0, buckets)}
   * @throws IllegalArgumentException if {@code buckets <= 0}
   */
  public static int bucket(long key, int buckets) {
    if (buckets <= 0) {
      throw new IllegalArgumentException("buckets must be > 0");
    }
    long k = key;
    long b = -1;
    long j = 0;
    while (j < buckets) {
      b = j;
      k = k * JUMP_MULTIPLIER + 1;
      double unsignedShift = (double) ((k >>> 33) + 1);
      j = (long) ((b + 1) * ((double) (1L << 31) / unsignedShift));
    }
    return (int) b;
  }

  /**
   * Hashes a string with 64-bit FNV-1a and assigns it to a bucket in {@code [0, buckets)}.
   *
   * @param key the string key (must not be null)
   * @param buckets the number of buckets (must be positive)
   * @return the bucket index in {@code [0, buckets)}
   * @throws NullPointerException if {@code key} is null
   * @throws IllegalArgumentException if {@code buckets <= 0}
   */
  public static int bucket(String key, int buckets) {
    Objects.requireNonNull(key, "key");
    return bucket(fnv1a64(key), buckets);
  }

  /**
   * Computes the 64-bit FNV-1a hash of a UTF-8 string.
   *
   * @param input the string to hash
   * @return the 64-bit hash value
   */
  static long fnv1a64(String input) {
    byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
    long hash = FNV_64_OFFSET_BASIS;
    for (byte b : bytes) {
      hash ^= (b & 0xffL);
      hash *= FNV_64_PRIME;
    }
    return hash;
  }

  /**
   * Routes a supported shard key object ({@link Number} or {@link String}) to a bucket.
   *
   * @param keyValue the parameter value bound to the shard key
   * @param buckets the number of shards
   * @return the shard index in {@code [0, buckets)}
   */
  static int bucketForParameter(Object keyValue, int buckets) {
    if (keyValue == null) {
      throw new ShardException("invalid_parameter", "shard key parameter must not be null");
    }
    if (keyValue instanceof Long
        || keyValue instanceof Integer
        || keyValue instanceof Short
        || keyValue instanceof Byte) {
      return bucket(((Number) keyValue).longValue(), buckets);
    }
    if (keyValue instanceof String s) {
      return bucket(s, buckets);
    }
    return bucket(keyValue.toString(), buckets);
  }
}
