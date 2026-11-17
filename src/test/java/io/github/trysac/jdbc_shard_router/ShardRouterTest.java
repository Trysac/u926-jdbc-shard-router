package io.github.trysac.jdbc_shard_router;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

/** Comprehensive unit, differential, and error-handling tests for {@link ShardRouter}. */
class ShardRouterTest {

  private static DataSource createH2DataSource(String dbName) {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
    ds.setUser("sa");
    ds.setPassword("");
    return ds;
  }

  private static void initOrdersSchema(DataSource ds) throws SQLException {
    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS orders");
      st.execute(
          "CREATE TABLE orders ("
              + "id INT PRIMARY KEY, "
              + "customer_id INT NOT NULL, "
              + "amount DECIMAL(12, 2)"
              + ")");
    }
  }

  @Test
  void jumpHashDistributionAndMonotonicity() {
    int movedCount = 0;
    for (long key = 0; key <= 9_999; key++) {
      int b4 = JumpHash.bucket(key, 4);
      int b5 = JumpHash.bucket(key, 5);
      assertTrue(b4 >= 0 && b4 < 4);
      assertTrue(b5 >= 0 && b5 < 5);
      if (b4 != b5) {
        assertEquals(4, b5, "Every moved key when going from 4 to 5 buckets must move to bucket 4");
        movedCount++;
      }
    }
    assertTrue(
        movedCount >= 1_500 && movedCount <= 2_500,
        "Expected between 1500 and 2500 moved keys, got: " + movedCount);

    assertEquals(0, JumpHash.bucket("customer-42", 1));
    int strBucket = JumpHash.bucket("customer-42", 4);
    assertTrue(strBucket >= 0 && strBucket < 4);
    assertThrows(IllegalArgumentException.class, () -> JumpHash.bucket(1L, 0));
    assertThrows(IllegalArgumentException.class, () -> JumpHash.bucket(1L, -3));
    assertThrows(NullPointerException.class, () -> JumpHash.bucket(null, 4));
  }

  @Test
  void routingPointLookupVersusScatterGatherAndDifferential12Queries() throws SQLException {
    String prefix = "diff_" + UUID.randomUUID().toString().replace('-', '_');
    List<List<String>> executedSqlPerShard = new ArrayList<>(4);
    List<DataSource> trackedShards = new ArrayList<>(4);

    for (int i = 0; i < 4; i++) {
      DataSource raw = createH2DataSource(prefix + "_shard" + i);
      initOrdersSchema(raw);
      List<String> sqlLog = new CopyOnWriteArrayList<>();
      executedSqlPerShard.add(sqlLog);
      trackedShards.add(wrapWithSqlRecorder(raw, sqlLog));
    }
    DataSource refDs = createH2DataSource(prefix + "_ref");
    initOrdersSchema(refDs);

    List<RoutedEvent> events = new CopyOnWriteArrayList<>();
    ShardRouter router =
        ShardRouter.builder()
            .shards(trackedShards)
            .shardKey("orders", "customer_id")
            .onEvent(events::add)
            .build();

    Random rng = new Random(7);
    for (int id = 1; id <= 1_000; id++) {
      int customerId = rng.nextInt(50) + 1;
      BigDecimal amount = BigDecimal.valueOf(rng.nextInt(100_000), 2);
      int updated =
          router.update(
              "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
              List.of(id, customerId, amount));
      assertEquals(1, updated);
      assertEquals("update", events.getLast().kind());
      assertEquals(1, events.getLast().shardCount());

      try (Connection refConn = refDs.getConnection();
          PreparedStatement ps =
              refConn.prepareStatement(
                  "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)")) {
        ps.setInt(1, id);
        ps.setInt(2, customerId);
        ps.setBigDecimal(3, amount);
        ps.executeUpdate();
      }
    }

    events.clear();
    // 1. Point lookup by key
    ResultTable q1 =
        router.query(
            "SELECT id, customer_id, amount FROM orders WHERE customer_id = ?", List.of(12));
    assertEquals(1, events.size());
    assertEquals("query", events.get(0).kind());
    assertEquals(1, events.get(0).shardCount());
    assertTrue(events.get(0).durationNanos() >= 0);
    ResultTable ref1 =
        queryReference(
            refDs, "SELECT id, customer_id, amount FROM orders WHERE customer_id = ?", List.of(12));
    assertSameMultiset(ref1, q1);

    // 2. Range on amount (scatter-gather to all 4 shards)
    events.clear();
    ResultTable q2 =
        router.query(
            "SELECT id, customer_id, amount FROM orders WHERE amount >= ? AND amount <= ?",
            List.of(new BigDecimal("100.00"), new BigDecimal("250.00")));
    assertEquals(1, events.size());
    assertEquals(4, events.get(0).shardCount());
    ResultTable ref2 =
        queryReference(
            refDs,
            "SELECT id, customer_id, amount FROM orders WHERE amount >= ? AND amount <= ?",
            List.of(new BigDecimal("100.00"), new BigDecimal("250.00")));
    assertSameMultiset(ref2, q2);

    // 3. COUNT(*)
    ResultTable q3 = router.query("SELECT COUNT(*) FROM orders", List.of());
    ResultTable ref3 = queryReference(refDs, "SELECT COUNT(*) FROM orders", List.of());
    assertSameOrdered(ref3, q3);
    assertEquals(1_000L, q3.rows().get(0).get(0));

    // 4. SUM(amount)
    ResultTable q4 = router.query("SELECT SUM(amount) FROM orders", List.of());
    ResultTable ref4 = queryReference(refDs, "SELECT SUM(amount) FROM orders", List.of());
    assertSameOrdered(ref4, q4);

    // 5. MIN(amount)
    ResultTable q5 = router.query("SELECT MIN(amount) FROM orders", List.of());
    ResultTable ref5 = queryReference(refDs, "SELECT MIN(amount) FROM orders", List.of());
    assertSameOrdered(ref5, q5);

    // 6. MAX(amount)
    ResultTable q6 = router.query("SELECT MAX(amount) FROM orders", List.of());
    ResultTable ref6 = queryReference(refDs, "SELECT MAX(amount) FROM orders", List.of());
    assertSameOrdered(ref6, q6);

    // 7. AVG(amount) with scale 10 and RoundingMode.HALF_EVEN
    ResultTable q7 = router.query("SELECT AVG(amount) FROM orders", List.of());
    BigDecimal refSum = (BigDecimal) ref4.rows().get(0).get(0);
    long refCount = ((Number) ref3.rows().get(0).get(0)).longValue();
    BigDecimal expectedAvg =
        refSum.divide(BigDecimal.valueOf(refCount), 10, RoundingMode.HALF_EVEN);
    assertEquals(List.of("AVG(amount)"), q7.columns());
    assertEquals(expectedAvg, q7.rows().get(0).get(0));

    // 8. GROUP BY customer_id with COUNT and SUM
    ResultTable q8 =
        router.query(
            "SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id",
            List.of());
    ResultTable ref8 =
        queryReference(
            refDs,
            "SELECT customer_id, COUNT(*), SUM(amount) FROM orders GROUP BY customer_id",
            List.of());
    assertSameMultiset(ref8, q8);

    // 9. ORDER BY amount DESC LIMIT 5 OFFSET 10 (plus tie-breaker id ASC in acceptance test)
    for (List<String> log : executedSqlPerShard) {
      log.clear();
    }
    String orderLimitSql =
        "SELECT customer_id, amount FROM orders ORDER BY amount DESC, id ASC LIMIT 5 OFFSET 10";
    ResultTable q9 = router.query(orderLimitSql, List.of());
    ResultTable ref9 = queryReference(refDs, orderLimitSql, List.of());
    assertEquals(5, q9.rows().size());
    assertSameOrdered(ref9, q9);
    for (List<String> log : executedSqlPerShard) {
      assertEquals(1, log.size());
      assertTrue(
          log.get(0).endsWith("LIMIT 15"),
          "Expected LIMIT 15 pushed down to each shard, got: " + log.get(0));
    }

    // 10. ORDER BY with two columns without LIMIT
    String orderTwoColsSql =
        "SELECT id, customer_id, amount FROM orders ORDER BY customer_id ASC, amount DESC";
    ResultTable q10 = router.query(orderTwoColsSql, List.of());
    ResultTable ref10 = queryReference(refDs, orderTwoColsSql, List.of());
    assertSameOrdered(ref10, q10);

    // 11. GROUP BY with ORDER BY and LIMIT
    String groupOrderLimitSql =
        "SELECT customer_id, COUNT(*), SUM(amount) FROM orders "
            + "GROUP BY customer_id ORDER BY customer_id DESC LIMIT 10 OFFSET 5";
    ResultTable q11 = router.query(groupOrderLimitSql, List.of());
    ResultTable ref11 = queryReference(refDs, groupOrderLimitSql, List.of());
    assertSameOrdered(ref11, q11);

    // 12. Empty result
    String emptySql = "SELECT id, customer_id, amount FROM orders WHERE amount < ?";
    ResultTable q12 = router.query(emptySql, List.of(new BigDecimal("-1.00")));
    ResultTable ref12 = queryReference(refDs, emptySql, List.of(new BigDecimal("-1.00")));
    assertTrue(q12.rows().isEmpty());
    assertSameOrdered(ref12, q12);
  }

  @Test
  void writeOperationsAndErrorCodes() throws SQLException {
    String prefix = "err_" + UUID.randomUUID().toString().replace('-', '_');
    List<DataSource> shards = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      DataSource ds = createH2DataSource(prefix + "_shard" + i);
      initOrdersSchema(ds);
      shards.add(ds);
    }

    ShardRouter router =
        ShardRouter.builder().shards(shards).shardKey("orders", "customer_id").build();

    // Single-shard INSERT, UPDATE, DELETE
    assertEquals(
        1,
        router.update(
            "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
            List.of(101, 7, new BigDecimal("50.00"))));
    assertEquals(
        1,
        router.update(
            "UPDATE orders SET amount = ? WHERE customer_id = ? AND id = ?",
            List.of(new BigDecimal("75.00"), 7, 101)));
    ResultTable afterUpdate =
        router.query("SELECT amount FROM orders WHERE customer_id = ?", List.of(7));
    assertEquals(new BigDecimal("75.00"), afterUpdate.rows().get(0).get(0));
    assertEquals(
        1, router.update("DELETE FROM orders WHERE customer_id = ? AND id = ?", List.of(7, 101)));
    assertTrue(
        router.query("SELECT id FROM orders WHERE customer_id = ?", List.of(7)).rows().isEmpty());

    // UPDATE without shard key in WHERE -> scatter_write_forbidden
    ShardException exUpdate =
        assertThrows(
            ShardException.class,
            () ->
                router.update(
                    "UPDATE orders SET amount = ? WHERE id = ?",
                    List.of(new BigDecimal("10.00"), 101)));
    assertEquals("scatter_write_forbidden", exUpdate.code());
    assertEquals(-1, exUpdate.shardIndex());

    // DELETE without shard key in WHERE -> scatter_write_forbidden
    ShardException exDelete =
        assertThrows(
            ShardException.class,
            () -> router.update("DELETE FROM orders WHERE id = ?", List.of(101)));
    assertEquals("scatter_write_forbidden", exDelete.code());

    // INSERT without shard key column -> missing_shard_key
    ShardException exInsert =
        assertThrows(
            ShardException.class,
            () ->
                router.update(
                    "INSERT INTO orders (id, amount) VALUES (?, ?)",
                    List.of(102, new BigDecimal("20.00"))));
    assertEquals("missing_shard_key", exInsert.code());

    // Unregistered table -> missing_shard_key
    ShardException exTable =
        assertThrows(
            ShardException.class,
            () -> router.query("SELECT id FROM unknown_table WHERE id = ?", List.of(1)));
    assertEquals("missing_shard_key", exTable.code());

    // JOIN or subquery -> unsupported_sql
    ShardException exJoin =
        assertThrows(
            ShardException.class,
            () ->
                router.query(
                    "SELECT id FROM orders JOIN customers ON orders.customer_id = customers.id",
                    List.of()));
    assertEquals("unsupported_sql", exJoin.code());

    ShardException exSubquery =
        assertThrows(
            ShardException.class,
            () ->
                router.query(
                    "SELECT id FROM orders WHERE customer_id = (SELECT id FROM customers)",
                    List.of()));
    assertEquals("unsupported_sql", exSubquery.code());

    // Statement of 16_385 characters -> sql_too_long
    String longSql = "SELECT id FROM orders WHERE customer_id = ?" + " ".repeat(16_385 - 43);
    assertEquals(16_385, longSql.length());
    ShardException exLong =
        assertThrows(ShardException.class, () -> router.query(longSql, List.of(1)));
    assertEquals("sql_too_long", exLong.code());
  }

  @Test
  void maxRowsExceededAndShardFailureWithoutLeakingParameters() throws SQLException {
    String prefix = "fail_" + UUID.randomUUID().toString().replace('-', '_');
    List<DataSource> shards = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      DataSource ds = createH2DataSource(prefix + "_shard" + i);
      initOrdersSchema(ds);
      shards.add(ds);
    }

    // Test maxRows (both default 100_000 check via small configured maxRows and direct > 100_000
    // simulation)
    ShardRouter smallLimitRouter =
        ShardRouter.builder().shards(shards).shardKey("orders", "customer_id").maxRows(3).build();
    for (int i = 1; i <= 5; i++) {
      smallLimitRouter.update(
          "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
          List.of(i, i, new BigDecimal("10.00")));
    }
    ShardException tooManySmall =
        assertThrows(
            ShardException.class, () -> smallLimitRouter.query("SELECT id FROM orders", List.of()));
    assertEquals("too_many_rows", tooManySmall.code());

    // Also verify default maxRows 100_000 throws too_many_rows when merged result exceeds 100_000
    DataSource syntheticShard = createSyntheticRowsDataSource(26_000);
    ShardRouter defaultLimitRouter =
        ShardRouter.builder()
            .shards(List.of(syntheticShard, syntheticShard, syntheticShard, syntheticShard))
            .shardKey("orders", "customer_id")
            .build();
    ShardException tooManyDefault =
        assertThrows(
            ShardException.class,
            () -> defaultLimitRouter.query("SELECT id FROM orders", List.of()));
    assertEquals("too_many_rows", tooManyDefault.code());

    // Test SQLException on shard 2 -> shard_failure with shardIndex() == 2 and no parameter values
    List<DataSource> failingShards = new ArrayList<>(shards);
    failingShards.set(2, new FailingDataSource("secret-db-detail"));
    ShardRouter failingRouter =
        ShardRouter.builder().shards(failingShards).shardKey("orders", "customer_id").build();

    // Numeric, so the healthy shards run the query fine and only shard 2 fails.
    BigDecimal sensitiveValue = new BigDecimal("987654321.99");
    String sensitiveParam = sensitiveValue.toPlainString();
    ShardException shardFail =
        assertThrows(
            ShardException.class,
            () ->
                failingRouter.query(
                    "SELECT id FROM orders WHERE amount > ?", List.of(sensitiveValue)));
    assertEquals("shard_failure", shardFail.code());
    assertEquals(2, shardFail.shardIndex());
    assertFalse(
        shardFail.getMessage().contains(sensitiveParam),
        "Exception message must never contain parameter values");
  }

  @Test
  void edgeCasesNullAmountsZeroRowsLimitZeroOffsetBeyondAndSingleShard() throws SQLException {
    String prefix = "edge_" + UUID.randomUUID().toString().replace('-', '_');
    List<DataSource> shards = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      DataSource ds = createH2DataSource(prefix + "_shard" + i);
      initOrdersSchema(ds);
      shards.add(ds);
    }
    ShardRouter router =
        ShardRouter.builder().shards(shards).shardKey("orders", "customer_id").build();

    // All shards empty: aggregates over 0 rows
    ResultTable emptyAgg =
        router.query(
            "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM orders",
            List.of());
    assertEquals(1, emptyAgg.rows().size());
    assertEquals(0L, emptyAgg.rows().get(0).get(0));
    assertNull(emptyAgg.rows().get(0).get(1));
    assertNull(emptyAgg.rows().get(0).get(2));
    assertNull(emptyAgg.rows().get(0).get(3));
    assertNull(emptyAgg.rows().get(0).get(4));

    // Insert rows onto only one shard (customer_id = 10), including NULL amounts
    router.update(
        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
        java.util.Arrays.asList(1, 10, null));
    router.update(
        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
        List.of(2, 10, new BigDecimal("10.00")));
    router.update(
        "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
        List.of(3, 10, new BigDecimal("20.00")));

    // Zero rows on 3 shards, 3 rows (one with NULL amount) on 1 shard
    ResultTable nullAgg =
        router.query(
            "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM orders",
            List.of());
    assertEquals(3L, nullAgg.rows().get(0).get(0));
    assertEquals(new BigDecimal("30.00"), nullAgg.rows().get(0).get(1));
    assertEquals(new BigDecimal("10.00"), nullAgg.rows().get(0).get(2));
    assertEquals(new BigDecimal("20.00"), nullAgg.rows().get(0).get(3));
    assertEquals(new BigDecimal("15.0000000000"), nullAgg.rows().get(0).get(4));

    // ORDER BY with NULL values (ASC and DESC)
    ResultTable orderNullAsc =
        router.query("SELECT id, amount FROM orders ORDER BY amount ASC", List.of());
    assertEquals(3, orderNullAsc.rows().size());
    assertNull(orderNullAsc.rows().get(0).get(1));

    ResultTable orderNullDesc =
        router.query("SELECT id, amount FROM orders ORDER BY amount DESC", List.of());
    assertEquals(3, orderNullDesc.rows().size());
    assertNull(orderNullDesc.rows().get(2).get(1));

    // LIMIT 0 and OFFSET beyond result
    ResultTable limitZero =
        router.query("SELECT id FROM orders ORDER BY id ASC LIMIT 0", List.of());
    assertTrue(limitZero.rows().isEmpty());

    ResultTable offsetBeyond =
        router.query("SELECT id FROM orders ORDER BY id ASC LIMIT 5 OFFSET 100", List.of());
    assertTrue(offsetBeyond.rows().isEmpty());

    // Single-shard router
    ShardRouter singleShardRouter =
        ShardRouter.builder()
            .shards(List.of(shards.get(0)))
            .shardKey("orders", "customer_id")
            .build();
    ResultTable singleRes = singleShardRouter.query("SELECT COUNT(*) FROM orders", List.of());
    assertNotNull(singleRes.rows().get(0).get(0));
  }

  @Test
  void stringShardKeyAndParameterValidation() throws SQLException {
    String prefix = "str_" + UUID.randomUUID().toString().replace('-', '_');
    DataSource ds = createH2DataSource(prefix);
    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE tenants (tenant_key VARCHAR(64) PRIMARY KEY, seats INT)");
    }
    ShardRouter router =
        ShardRouter.builder().shards(List.of(ds)).shardKey("tenants", "tenant_key").build();

    assertEquals(
        1,
        router.update(
            "INSERT INTO tenants (tenant_key, seats) VALUES (?, ?)", List.of("acme-corp", 25)));
    ResultTable res =
        router.query(
            "SELECT tenant_key, seats FROM tenants WHERE tenant_key = ?", List.of("acme-corp"));
    assertEquals(1, res.rows().size());
    assertEquals(25, res.rows().get(0).get(1));

    // Parameter errors
    ShardException nullParams =
        assertThrows(
            ShardException.class,
            () -> router.query("SELECT seats FROM tenants WHERE tenant_key = ?", null));
    assertEquals("invalid_parameter", nullParams.code());

    ShardException wrongCount =
        assertThrows(
            ShardException.class,
            () -> router.query("SELECT seats FROM tenants WHERE tenant_key = ?", List.of()));
    assertEquals("invalid_parameter", wrongCount.code());

    ShardException nullShardKey =
        assertThrows(
            ShardException.class,
            () ->
                router.query(
                    "SELECT seats FROM tenants WHERE tenant_key = ?",
                    java.util.Collections.singletonList(null)));
    assertEquals("invalid_parameter", nullShardKey.code());
  }

  private static DataSource wrapWithSqlRecorder(DataSource delegate, List<String> sqlLog) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                Connection rawConn = (Connection) method.invoke(delegate, args);
                return wrapConnection(rawConn, sqlLog);
              }
              return method.invoke(delegate, args);
            });
  }

  private static Connection wrapConnection(Connection delegate, List<String> sqlLog) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("prepareStatement") && args != null && args.length > 0) {
                sqlLog.add((String) args[0]);
              }
              return method.invoke(delegate, args);
            });
  }

  private static DataSource createSyntheticRowsDataSource(int rowCount) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (dsProxy, dsMethod, dsArgs) -> {
              if (dsMethod.getName().equals("getConnection")) {
                return Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (connProxy, connMethod, connArgs) -> {
                      if (connMethod.getName().equals("prepareStatement")) {
                        return Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            (psProxy, psMethod, psArgs) -> {
                              if (psMethod.getName().equals("executeQuery")) {
                                final int[] cursor = {0};
                                return Proxy.newProxyInstance(
                                    ResultSet.class.getClassLoader(),
                                    new Class<?>[] {ResultSet.class},
                                    (rsProxy, rsMethod, rsArgs) -> {
                                      if (rsMethod.getName().equals("next")) {
                                        cursor[0]++;
                                        return cursor[0] <= rowCount;
                                      }
                                      if (rsMethod.getName().equals("getObject")) {
                                        return cursor[0];
                                      }
                                      return null;
                                    });
                              }
                              return null;
                            });
                      }
                      return null;
                    });
              }
              return null;
            });
  }

  private static final class FailingDataSource implements DataSource {
    private final String detail;

    FailingDataSource(String detail) {
      this.detail = detail;
    }

    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("Simulated connection failure: " + detail, "08001");
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLException("Simulated connection failure: " + detail, "08001");
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("unwrap not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  private static ResultTable queryReference(DataSource ds, String sql, List<Object> params)
      throws SQLException {
    SqlAst.Statement parsed = SqlParser.parse(sql);
    List<String> cols = new ArrayList<>();
    if (parsed instanceof SqlAst.SelectStmt sel) {
      for (SqlAst.SelectItem item : sel.items()) {
        if (item instanceof SqlAst.ColumnItem c) {
          cols.add(c.column().toLowerCase(java.util.Locale.ROOT));
        } else if (item instanceof SqlAst.AggregateItem a) {
          cols.add(a.label());
        }
      }
    }
    try (Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        ps.setObject(i + 1, params.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        int colCount = rs.getMetaData().getColumnCount();
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
          List<Object> r = new ArrayList<>(colCount);
          for (int c = 1; c <= colCount; c++) {
            r.add(rs.getObject(c));
          }
          rows.add(r);
        }
        return new ResultTable(cols, rows);
      }
    }
  }

  private static void assertSameOrdered(ResultTable expected, ResultTable actual) {
    assertEquals(expected.columns(), actual.columns());
    assertEquals(expected.rows(), actual.rows());
  }

  private static void assertSameMultiset(ResultTable expected, ResultTable actual) {
    assertEquals(expected.columns(), actual.columns());
    Comparator<List<Object>> lexComp =
        (r1, r2) -> {
          for (int i = 0; i < r1.size(); i++) {
            String s1 = String.valueOf(r1.get(i));
            String s2 = String.valueOf(r2.get(i));
            int cmp = s1.compareTo(s2);
            if (cmp != 0) {
              return cmp;
            }
          }
          return 0;
        };
    List<List<Object>> sortedExpected = new ArrayList<>(expected.rows());
    List<List<Object>> sortedActual = new ArrayList<>(actual.rows());
    sortedExpected.sort(lexComp);
    sortedActual.sort(lexComp);
    assertEquals(sortedExpected, sortedActual);
  }
}
