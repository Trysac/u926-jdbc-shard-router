package io.github.trysac.jdbc_shard_router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;

/** Property-based tests for {@link Text} and {@link JumpHash}. */
class TextPropertiesTest {

  @Property
  void normalizeIsIdempotent(@ForAll @AlphaChars @StringLength(min = 1, max = 40) String word) {
    String once = Text.normalize("  " + word + "\n");
    assertEquals(once, Text.normalize(once));
  }

  @Property
  void jumpHashAlwaysInRangeAndConsistent(
      @ForAll long key, @ForAll @IntRange(min = 1, max = 1024) int buckets) {
    int b = JumpHash.bucket(key, buckets);
    assertTrue(b >= 0 && b < buckets);
    if (buckets > 1) {
      int prev = JumpHash.bucket(key, buckets - 1);
      assertTrue(b == prev || b == buckets - 1);
    }
  }
}
