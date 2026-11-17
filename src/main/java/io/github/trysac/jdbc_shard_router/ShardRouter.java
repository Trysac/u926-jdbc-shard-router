package io.github.trysac.jdbc_shard_router;

import io.github.trysac.jdbc_shard_router.SqlAst.AggFunc;
import io.github.trysac.jdbc_shard_router.SqlAst.AggregateItem;
import io.github.trysac.jdbc_shard_router.SqlAst.ColumnItem;
import io.github.trysac.jdbc_shard_router.SqlAst.Condition;
import io.github.trysac.jdbc_shard_router.SqlAst.DeleteStmt;
import io.github.trysac.jdbc_shard_router.SqlAst.InsertStmt;
import io.github.trysac.jdbc_shard_router.SqlAst.OrderSpec;
import io.github.trysac.jdbc_shard_router.SqlAst.SelectItem;
import io.github.trysac.jdbc_shard_router.SqlAst.SelectStmt;
import io.github.trysac.jdbc_shard_router.SqlAst.SetClause;
import io.github.trysac.jdbc_shard_router.SqlAst.Statement;
import io.github.trysac.jdbc_shard_router.SqlAst.UpdateStmt;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.sql.DataSource;

/**
 * Routes SQL queries and updates across a fixed list of JDBC {@link DataSource} shards using Jump
 * Consistent Hashing, and merges scatter-gather query results.
 */
public final class ShardRouter {

  /** Default maximum number of rows returned or buffered during a merge (100,000). */
  public static final int DEFAULT_MAX_ROWS = 100_000;

  private final List<DataSource> shards;
  private final Map<String, String> shardKeys;
  private final int maxRows;
  private final Consumer<RoutedEvent> onEvent;
  private final ExecutorService executor;

  private ShardRouter(
      List<DataSource> shards,
      Map<String, String> shardKeys,
      int maxRows,
      Consumer<RoutedEvent> onEvent) {
    this.shards = List.copyOf(shards);
    this.shardKeys = Map.copyOf(shardKeys);
    this.maxRows = maxRows;
    this.onEvent = onEvent;
    AtomicInteger threadIdx = new AtomicInteger(0);
    this.executor =
        Executors.newFixedThreadPool(
            this.shards.size(),
            r -> {
              Thread t = new Thread(r, "jdbc-shard-router-" + threadIdx.getAndIncrement());
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Creates a new {@link Builder} for configuring a {@link ShardRouter}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ShardRouter}. */
  public static final class Builder {
    private List<DataSource> shards = List.of();
    private final Map<String, String> shardKeys = new HashMap<>();
    private int maxRows = DEFAULT_MAX_ROWS;
    private Consumer<RoutedEvent> onEvent = event -> {};

    private Builder() {}

    /**
     * Sets the ordered list of shard {@link DataSource} instances.
     *
     * @param shards non-empty list of data sources
     * @return this builder
     */
    public Builder shards(List<DataSource> shards) {
      Objects.requireNonNull(shards, "shards");
      if (shards.isEmpty()) {
        throw new IllegalArgumentException("shards must not be empty");
      }
      for (DataSource ds : shards) {
        Objects.requireNonNull(ds, "shard DataSource must not be null");
      }
      this.shards = List.copyOf(shards);
      return this;
    }

    /**
     * Registers the shard key column for a table.
     *
     * @param table the table name
     * @param column the shard key column name
     * @return this builder
     */
    public Builder shardKey(String table, String column) {
      String normTable = Text.normalize(table);
      String normCol = Text.normalize(column);
      this.shardKeys.put(normTable, normCol);
      return this;
    }

    /**
     * Sets the maximum number of merged rows allowed before throwing {@code too_many_rows}.
     *
     * @param maxRows positive row limit (default 100,000)
     * @return this builder
     */
    public Builder maxRows(int maxRows) {
      if (maxRows <= 0) {
        throw new IllegalArgumentException("maxRows must be > 0");
      }
      this.maxRows = maxRows;
      return this;
    }

    /**
     * Sets a callback invoked after every routed query or update.
     *
     * @param onEvent callback receiving {@link RoutedEvent}
     * @return this builder
     */
    public Builder onEvent(Consumer<RoutedEvent> onEvent) {
      this.onEvent = Objects.requireNonNull(onEvent, "onEvent");
      return this;
    }

    /**
     * Builds the {@link ShardRouter}.
     *
     * @return the configured router
     */
    public ShardRouter build() {
      if (shards.isEmpty()) {
        throw new IllegalStateException("at least one shard DataSource must be configured");
      }
      return new ShardRouter(shards, shardKeys, maxRows, onEvent);
    }
  }

  /**
   * Executes a {@code SELECT} query against one shard (when constrained by {@code shard_key = ?})
   * or all shards in parallel, merging the results.
   *
   * @param sql the SQL SELECT statement
   * @param params positional parameters matching {@code ?} placeholders
   * @return the merged {@link ResultTable}
   * @throws ShardException on SQL, routing, parameter, row-limit, or JDBC failure
   */
  public ResultTable query(String sql, List<Object> params) {
    long startNanos = System.nanoTime();
    Statement stmt = SqlParser.parse(sql);
    if (!(stmt instanceof SelectStmt select)) {
      throw new ShardException("unsupported_sql", "query() only accepts SELECT statements");
    }
    List<Object> safeParams = validateParams(params, select.parameterCount());
    String keyCol = requireTableShardKey(select.table());

    Integer targetShard = findTargetShardFromWhere(select.where(), keyCol, safeParams);
    QueryPlan plan = buildQueryPlan(select);

    List<List<List<Object>>> shardRows;
    int contactedShards;
    if (targetShard != null) {
      contactedShards = 1;
      List<List<Object>> single =
          executeSelectOnShard(targetShard, plan.shardSql(), safeParams, plan.shardColumnCount());
      shardRows = List.of(single);
    } else {
      contactedShards = shards.size();
      shardRows = executeScatterSelect(plan.shardSql(), safeParams, plan.shardColumnCount());
    }

    ResultTable result = mergeSelectResults(select, plan, shardRows);
    long duration = System.nanoTime() - startNanos;
    onEvent.accept(new RoutedEvent("query", contactedShards, duration));
    return result;
  }

  /**
   * Executes an {@code INSERT}, {@code UPDATE}, or {@code DELETE} statement on the single shard
   * determined by the table's shard key.
   *
   * @param sql the SQL DML statement
   * @param params positional parameters matching {@code ?} placeholders
   * @return the number of affected rows
   * @throws ShardException on SQL, routing, missing shard key, scatter write, or JDBC failure
   */
  public int update(String sql, List<Object> params) {
    long startNanos = System.nanoTime();
    Statement stmt = SqlParser.parse(sql);
    int targetShard;
    List<Object> safeParams;
    switch (stmt) {
      case InsertStmt insert -> {
        safeParams = validateParams(params, insert.parameterCount());
        String keyCol = requireTableShardKey(insert.table());
        int keyIdx = -1;
        for (int i = 0; i < insert.columns().size(); i++) {
          if (insert.columns().get(i).equalsIgnoreCase(keyCol)) {
            keyIdx = i;
            break;
          }
        }
        if (keyIdx < 0) {
          throw new ShardException(
              "missing_shard_key", "INSERT is missing shard key column: " + keyCol);
        }
        targetShard = JumpHash.bucketForParameter(safeParams.get(keyIdx), shards.size());
      }
      case UpdateStmt upd -> {
        safeParams = validateParams(params, upd.parameterCount());
        String keyCol = requireTableShardKey(upd.table());
        for (SetClause set : upd.sets()) {
          if (set.column().equalsIgnoreCase(keyCol)) {
            throw new ShardException(
                "unsupported_sql", "updating the shard key column is not supported");
          }
        }
        Integer shard = findTargetShardFromWhere(upd.where(), keyCol, safeParams);
        if (shard == null) {
          throw new ShardException(
              "scatter_write_forbidden",
              "UPDATE must constrain shard key " + keyCol + " with '= ?'");
        }
        targetShard = shard;
      }
      case DeleteStmt del -> {
        safeParams = validateParams(params, del.parameterCount());
        String keyCol = requireTableShardKey(del.table());
        Integer shard = findTargetShardFromWhere(del.where(), keyCol, safeParams);
        if (shard == null) {
          throw new ShardException(
              "scatter_write_forbidden",
              "DELETE must constrain shard key " + keyCol + " with '= ?'");
        }
        targetShard = shard;
      }
      case SelectStmt ignored ->
          throw new ShardException("unsupported_sql", "update() does not accept SELECT statements");
    }

    int updated = executeUpdateOnShard(targetShard, sql, safeParams);
    long duration = System.nanoTime() - startNanos;
    onEvent.accept(new RoutedEvent("update", 1, duration));
    return updated;
  }

  private List<Object> validateParams(List<Object> params, int expectedCount) {
    if (params == null) {
      throw new ShardException("invalid_parameter", "params list must not be null");
    }
    if (params.size() != expectedCount) {
      throw new ShardException(
          "invalid_parameter",
          "expected " + expectedCount + " parameters but got " + params.size());
    }
    return params;
  }

  private String requireTableShardKey(String table) {
    String key = shardKeys.get(table.toLowerCase(Locale.ROOT));
    if (key == null) {
      throw new ShardException("missing_shard_key", "no shard key configured for table: " + table);
    }
    return key;
  }

  private Integer findTargetShardFromWhere(
      List<Condition> where, String keyCol, List<Object> params) {
    Integer targetShard = null;
    for (Condition cond : where) {
      if (cond.column().equalsIgnoreCase(keyCol) && cond.op().equals("=")) {
        Object val = params.get(cond.paramIndex());
        int b = JumpHash.bucketForParameter(val, shards.size());
        if (targetShard == null) {
          targetShard = b;
        }
      }
    }
    return targetShard;
  }

  private record AvgPairs(int sumIdx, int countIdx) {}

  private record QueryPlan(
      String shardSql,
      int shardColumnCount,
      List<String> outputColumns,
      boolean hasAggregates,
      boolean hasGroupBy,
      Map<Integer, AvgPairs> avgPositions,
      List<Integer> orderColumnIndices,
      int projectedColumnCount) {}

  private QueryPlan buildQueryPlan(SelectStmt select) {
    boolean hasAgg = select.items().stream().anyMatch(item -> item instanceof AggregateItem);
    boolean hasGroupBy = !select.groupBy().isEmpty();

    List<String> outputColumns = new ArrayList<>(select.items().size());
    List<String> shardSelectExprs = new ArrayList<>();
    Map<Integer, AvgPairs> avgPositions = new HashMap<>();

    for (int i = 0; i < select.items().size(); i++) {
      SelectItem item = select.items().get(i);
      switch (item) {
        case ColumnItem col -> {
          outputColumns.add(col.column().toLowerCase(Locale.ROOT));
          shardSelectExprs.add(col.column());
        }
        case AggregateItem agg -> {
          outputColumns.add(agg.label());
          if (agg.func() == AggFunc.AVG) {
            int sumIdx = shardSelectExprs.size();
            shardSelectExprs.add("SUM(" + agg.column() + ")");
            int countIdx = shardSelectExprs.size();
            shardSelectExprs.add("COUNT(" + agg.column() + ")");
            avgPositions.put(i, new AvgPairs(sumIdx, countIdx));
          } else {
            shardSelectExprs.add(agg.func().name() + "(" + agg.column() + ")");
          }
        }
      }
    }

    int projectedCount = shardSelectExprs.size();
    List<Integer> orderIndices = new ArrayList<>(select.orderBy().size());

    for (OrderSpec ord : select.orderBy()) {
      int idx = findOutputColumnIndex(outputColumns, ord.column());
      if (idx < 0) {
        if (hasGroupBy || hasAgg) {
          throw new ShardException(
              "unsupported_sql", "ORDER BY column must be in SELECT list for grouped query");
        }
        int extraIdx = shardSelectExprs.size();
        shardSelectExprs.add(ord.column());
        orderIndices.add(extraIdx);
      } else {
        orderIndices.add(idx);
      }
    }

    StringBuilder sb = new StringBuilder("SELECT ");
    sb.append(String.join(", ", shardSelectExprs));
    sb.append(" FROM ").append(select.table());

    if (!select.where().isEmpty()) {
      sb.append(" WHERE ");
      for (int i = 0; i < select.where().size(); i++) {
        if (i > 0) {
          sb.append(" AND ");
        }
        Condition c = select.where().get(i);
        sb.append(c.column()).append(' ').append(c.op()).append(" ?");
      }
    }

    if (hasGroupBy) {
      sb.append(" GROUP BY ").append(String.join(", ", select.groupBy()));
    }

    if (!hasGroupBy && !hasAgg && !select.orderBy().isEmpty()) {
      sb.append(" ORDER BY ");
      for (int i = 0; i < select.orderBy().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        OrderSpec o = select.orderBy().get(i);
        sb.append(o.column()).append(o.ascending() ? " ASC" : " DESC");
      }
    }

    if (!hasGroupBy && !hasAgg && select.limit() != null) {
      int limitVal = select.limit();
      int offsetVal = select.offset() != null ? select.offset() : 0;
      long pushdownLimit = (long) limitVal + (long) offsetVal;
      sb.append(" LIMIT ").append(pushdownLimit);
    }

    return new QueryPlan(
        sb.toString(),
        shardSelectExprs.size(),
        List.copyOf(outputColumns),
        hasAgg,
        hasGroupBy,
        Map.copyOf(avgPositions),
        List.copyOf(orderIndices),
        projectedCount);
  }

  private int findOutputColumnIndex(List<String> outputColumns, String colName) {
    for (int i = 0; i < outputColumns.size(); i++) {
      if (outputColumns.get(i).equalsIgnoreCase(colName)) {
        return i;
      }
    }
    return -1;
  }

  private List<List<List<Object>>> executeScatterSelect(
      String shardSql, List<Object> params, int colCount) {
    if (shards.size() == 1) {
      return List.of(executeSelectOnShard(0, shardSql, params, colCount));
    }
    List<Callable<List<List<Object>>>> tasks = new ArrayList<>(shards.size());
    for (int i = 0; i < shards.size(); i++) {
      final int shardIdx = i;
      tasks.add(() -> executeSelectOnShard(shardIdx, shardSql, params, colCount));
    }
    List<Future<List<List<Object>>>> futures = new ArrayList<>(shards.size());
    for (Callable<List<List<Object>>> task : tasks) {
      futures.add(executor.submit(task));
    }
    ShardException firstFailure = null;
    List<List<List<Object>>> allResults = new ArrayList<>(shards.size());
    for (int i = 0; i < futures.size(); i++) {
      Future<List<List<Object>>> f = futures.get(i);
      try {
        List<List<Object>> res = f.get();
        if (firstFailure == null) {
          allResults.add(res);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (firstFailure == null) {
          firstFailure =
              new ShardException("shard_failure", "interrupted waiting for shard query", -1, e);
        }
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        ShardException se;
        if (cause instanceof ShardException existingSe) {
          se = existingSe;
        } else {
          se = new ShardException("shard_failure", "shard execution failed", i, cause);
        }
        if (firstFailure == null
            || (firstFailure.shardIndex() < 0 && se.shardIndex() >= 0)
            || (firstFailure.shardIndex() >= 0
                && se.shardIndex() >= 0
                && se.shardIndex() < firstFailure.shardIndex())) {
          firstFailure = se;
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
    return allResults;
  }

  private List<List<Object>> executeSelectOnShard(
      int shardIdx, String shardSql, List<Object> params, int colCount) {
    DataSource ds = shards.get(shardIdx);
    try (Connection conn = ds.getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(shardSql)) {
        bindParams(ps, params);
        try (ResultSet rs = ps.executeQuery()) {
          List<List<Object>> rows = new ArrayList<>();
          while (rs.next()) {
            if (rows.size() > maxRows) {
              throw new ShardException("too_many_rows", "shard result exceeds maxRows " + maxRows);
            }
            List<Object> row = new ArrayList<>(colCount);
            for (int c = 1; c <= colCount; c++) {
              row.add(rs.getObject(c));
            }
            rows.add(row);
          }
          return rows;
        }
      }
    } catch (SQLException e) {
      throw new ShardException(
          "shard_failure",
          "database error on shard " + shardIdx + " (SQLState=" + e.getSQLState() + ")",
          shardIdx,
          e);
    }
  }

  private int executeUpdateOnShard(int shardIdx, String sql, List<Object> params) {
    DataSource ds = shards.get(shardIdx);
    try (Connection conn = ds.getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindParams(ps, params);
        return ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw new ShardException(
          "shard_failure",
          "database error on shard " + shardIdx + " (SQLState=" + e.getSQLState() + ")",
          shardIdx,
          e);
    }
  }

  private void bindParams(PreparedStatement ps, List<Object> params) throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      ps.setObject(i + 1, params.get(i));
    }
  }

  private ResultTable mergeSelectResults(
      SelectStmt select, QueryPlan plan, List<List<List<Object>>> shardRows) {
    if (plan.hasGroupBy()) {
      return mergeGroupByResults(select, plan, shardRows);
    }
    if (plan.hasAggregates()) {
      return mergeGlobalAggregates(select, plan, shardRows);
    }
    return mergePlainSelectResults(select, plan, shardRows);
  }

  private ResultTable mergeGlobalAggregates(
      SelectStmt select, QueryPlan plan, List<List<List<Object>>> shardRows) {
    int offset = select.offset() != null ? select.offset() : 0;
    int limit = select.limit() != null ? select.limit() : Integer.MAX_VALUE;
    if (limit == 0 || offset > 0) {
      return new ResultTable(plan.outputColumns(), List.of());
    }

    List<List<Object>> flatRows = new ArrayList<>(shardRows.size());
    for (List<List<Object>> sr : shardRows) {
      flatRows.addAll(sr);
    }
    List<Object> mergedRow = computeAggregateRow(select.items(), plan, flatRows);
    return new ResultTable(plan.outputColumns(), List.of(mergedRow));
  }

  private ResultTable mergeGroupByResults(
      SelectStmt select, QueryPlan plan, List<List<List<Object>>> shardRows) {
    List<Integer> groupKeyPositions = new ArrayList<>(select.groupBy().size());
    for (String gCol : select.groupBy()) {
      int pos = -1;
      int rawIdx = 0;
      for (SelectItem item : select.items()) {
        if (item instanceof ColumnItem col && col.column().equalsIgnoreCase(gCol)) {
          pos = rawIdx;
          break;
        }
        rawIdx += (item instanceof AggregateItem agg && agg.func() == AggFunc.AVG) ? 2 : 1;
      }
      groupKeyPositions.add(pos);
    }

    Map<List<Object>, List<List<Object>>> groups = new LinkedHashMap<>();
    for (List<List<Object>> sr : shardRows) {
      for (List<Object> row : sr) {
        List<Object> key = new ArrayList<>(groupKeyPositions.size());
        for (int pos : groupKeyPositions) {
          key.add(normalizeKeyComponent(row.get(pos)));
        }
        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
    }

    if (groups.size() > maxRows) {
      throw new ShardException(
          "too_many_rows", "merged GROUP BY row count exceeds maxRows " + maxRows);
    }

    List<List<Object>> mergedRows = new ArrayList<>(groups.size());
    for (List<List<Object>> groupRows : groups.values()) {
      mergedRows.add(computeAggregateRow(select.items(), plan, groupRows));
    }

    if (!select.orderBy().isEmpty()) {
      Comparator<List<Object>> comp =
          buildRowComparator(select.orderBy(), plan.orderColumnIndices());
      mergedRows.sort(comp);
    }

    List<List<Object>> paged = applyLimitOffset(mergedRows, select.limit(), select.offset());
    return new ResultTable(plan.outputColumns(), paged);
  }

  private Object normalizeKeyComponent(Object val) {
    if (val instanceof Number n) {
      if (val instanceof BigDecimal bd) {
        return bd.stripTrailingZeros();
      }
      return n.longValue();
    }
    return val;
  }

  private List<Object> computeAggregateRow(
      List<SelectItem> items, QueryPlan plan, List<List<Object>> rows) {
    List<Object> out = new ArrayList<>(items.size());
    int rawCursor = 0;
    for (int i = 0; i < items.size(); i++) {
      SelectItem item = items.get(i);
      switch (item) {
        case ColumnItem ignored -> {
          Object val = rows.isEmpty() ? null : rows.get(0).get(rawCursor);
          out.add(val);
          rawCursor += 1;
        }
        case AggregateItem agg -> {
          switch (agg.func()) {
            case COUNT -> {
              long total = 0L;
              for (List<Object> r : rows) {
                Object v = r.get(rawCursor);
                if (v != null) {
                  total += ((Number) v).longValue();
                }
              }
              out.add(total);
              rawCursor += 1;
            }
            case SUM -> {
              BigDecimal sum = null;
              boolean anyIntegral = false;
              boolean anyDecimal = false;
              for (List<Object> r : rows) {
                Object v = r.get(rawCursor);
                if (v != null) {
                  if (v instanceof BigDecimal || v instanceof Double || v instanceof Float) {
                    anyDecimal = true;
                  } else {
                    anyIntegral = true;
                  }
                  BigDecimal bd = toBigDecimal(v);
                  sum = (sum == null) ? bd : sum.add(bd);
                }
              }
              if (sum == null) {
                out.add(null);
              } else if (anyIntegral && !anyDecimal) {
                out.add(sum.longValueExact());
              } else {
                out.add(sum);
              }
              rawCursor += 1;
            }
            case MIN -> {
              Object minVal = null;
              for (List<Object> r : rows) {
                Object v = r.get(rawCursor);
                if (v != null && (minVal == null || compareValues(v, minVal) < 0)) {
                  minVal = v;
                }
              }
              out.add(minVal);
              rawCursor += 1;
            }
            case MAX -> {
              Object maxVal = null;
              for (List<Object> r : rows) {
                Object v = r.get(rawCursor);
                if (v != null && (maxVal == null || compareValues(v, maxVal) > 0)) {
                  maxVal = v;
                }
              }
              out.add(maxVal);
              rawCursor += 1;
            }
            case AVG -> {
              AvgPairs pair = plan.avgPositions().get(i);
              BigDecimal totalSum = BigDecimal.ZERO;
              long totalCount = 0L;
              for (List<Object> r : rows) {
                Object sObj = r.get(pair.sumIdx());
                Object cObj = r.get(pair.countIdx());
                long cVal = cObj == null ? 0L : ((Number) cObj).longValue();
                if (cVal > 0 && sObj != null) {
                  totalSum = totalSum.add(toBigDecimal(sObj));
                  totalCount += cVal;
                }
              }
              if (totalCount == 0L) {
                out.add(null);
              } else {
                out.add(
                    totalSum.divide(BigDecimal.valueOf(totalCount), 10, RoundingMode.HALF_EVEN));
              }
              rawCursor += 2;
            }
          }
        }
      }
    }
    return out;
  }

  private ResultTable mergePlainSelectResults(
      SelectStmt select, QueryPlan plan, List<List<List<Object>>> shardRows) {
    long totalCount = 0;
    for (List<List<Object>> sr : shardRows) {
      totalCount += sr.size();
    }
    if (totalCount > maxRows && select.limit() == null) {
      throw new ShardException(
          "too_many_rows", "merged result count " + totalCount + " exceeds maxRows " + maxRows);
    }

    List<List<Object>> merged;
    if (!select.orderBy().isEmpty()) {
      merged = kWayMerge(shardRows, select.orderBy(), plan.orderColumnIndices());
    } else {
      merged = new ArrayList<>((int) Math.min(totalCount, maxRows + 1L));
      for (List<List<Object>> sr : shardRows) {
        for (List<Object> row : sr) {
          if (merged.size() >= maxRows) {
            throw new ShardException("too_many_rows", "merged result exceeds maxRows " + maxRows);
          }
          merged.add(row);
        }
      }
    }

    List<List<Object>> paged = applyLimitOffset(merged, select.limit(), select.offset());
    if (paged.size() > maxRows) {
      throw new ShardException("too_many_rows", "merged result exceeds maxRows " + maxRows);
    }

    int projCount = plan.projectedColumnCount();
    List<List<Object>> trimmed = new ArrayList<>(paged.size());
    for (List<Object> row : paged) {
      if (row.size() == projCount) {
        trimmed.add(row);
      } else {
        trimmed.add(new ArrayList<>(row.subList(0, projCount)));
      }
    }
    return new ResultTable(plan.outputColumns(), trimmed);
  }

  private record HeapItem(int shardIdx, int rowIdx, List<Object> row) {}

  private List<List<Object>> kWayMerge(
      List<List<List<Object>>> shardRows, List<OrderSpec> orderBy, List<Integer> orderIndices) {
    Comparator<List<Object>> rowComp = buildRowComparator(orderBy, orderIndices);
    PriorityQueue<HeapItem> pq =
        new PriorityQueue<>(
            Math.max(1, shardRows.size()),
            (a, b) -> {
              int cmp = rowComp.compare(a.row(), b.row());
              if (cmp != 0) {
                return cmp;
              }
              return Integer.compare(a.shardIdx(), b.shardIdx());
            });

    int total = 0;
    for (int s = 0; s < shardRows.size(); s++) {
      List<List<Object>> rows = shardRows.get(s);
      total += rows.size();
      if (!rows.isEmpty()) {
        pq.add(new HeapItem(s, 0, rows.get(0)));
      }
    }

    List<List<Object>> result = new ArrayList<>(total);
    while (!pq.isEmpty()) {
      HeapItem top = pq.poll();
      result.add(top.row());
      List<List<Object>> sRows = shardRows.get(top.shardIdx());
      int nextIdx = top.rowIdx() + 1;
      if (nextIdx < sRows.size()) {
        pq.add(new HeapItem(top.shardIdx(), nextIdx, sRows.get(nextIdx)));
      }
    }
    return result;
  }

  private Comparator<List<Object>> buildRowComparator(
      List<OrderSpec> orderBy, List<Integer> orderIndices) {
    return (r1, r2) -> {
      for (int i = 0; i < orderBy.size(); i++) {
        OrderSpec spec = orderBy.get(i);
        int colIdx = orderIndices.get(i);
        Object v1 = r1.get(colIdx);
        Object v2 = r2.get(colIdx);
        if (v1 == null && v2 == null) {
          continue;
        }
        if (v1 == null) {
          return spec.ascending() ? -1 : 1;
        }
        if (v2 == null) {
          return spec.ascending() ? 1 : -1;
        }
        int cmp = compareValues(v1, v2);
        if (cmp != 0) {
          return spec.ascending() ? cmp : -cmp;
        }
      }
      return 0;
    };
  }

  private List<List<Object>> applyLimitOffset(
      List<List<Object>> rows, Integer limit, Integer offset) {
    if (limit == null) {
      return rows;
    }
    int off = offset != null ? offset : 0;
    int lim = limit;
    if (lim == 0 || off >= rows.size()) {
      return Collections.emptyList();
    }
    int end = (int) Math.min((long) rows.size(), (long) off + (long) lim);
    return rows.subList(off, end);
  }

  private static BigDecimal toBigDecimal(Object val) {
    if (val instanceof BigDecimal bd) {
      return bd;
    }
    if (val instanceof Long
        || val instanceof Integer
        || val instanceof Short
        || val instanceof Byte) {
      return BigDecimal.valueOf(((Number) val).longValue());
    }
    if (val instanceof Double || val instanceof Float) {
      return BigDecimal.valueOf(((Number) val).doubleValue());
    }
    return new BigDecimal(val.toString());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compareValues(Object a, Object b) {
    if (a instanceof Number && b instanceof Number) {
      return toBigDecimal(a).compareTo(toBigDecimal(b));
    }
    if (a instanceof Comparable c1 && a.getClass().isInstance(b)) {
      return c1.compareTo(b);
    }
    return a.toString().compareTo(b.toString());
  }
}
