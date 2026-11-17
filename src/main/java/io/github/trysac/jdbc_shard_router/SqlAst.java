package io.github.trysac.jdbc_shard_router;

import java.util.List;

/** Internal AST definitions for the supported SQL subset. */
final class SqlAst {

  private SqlAst() {}

  sealed interface Statement permits SelectStmt, InsertStmt, UpdateStmt, DeleteStmt {
    String table();
  }

  enum AggFunc {
    COUNT,
    SUM,
    MIN,
    MAX,
    AVG
  }

  sealed interface SelectItem permits ColumnItem, AggregateItem {}

  record ColumnItem(String column) implements SelectItem {}

  record AggregateItem(AggFunc func, String column) implements SelectItem {
    String label() {
      return func.name() + "(" + column + ")";
    }
  }

  record Condition(String column, String op, int paramIndex) {}

  record OrderSpec(String column, boolean ascending) {}

  record SelectStmt(
      List<SelectItem> items,
      String table,
      List<Condition> where,
      List<String> groupBy,
      List<OrderSpec> orderBy,
      Integer limit,
      Integer offset,
      int parameterCount)
      implements Statement {}

  record InsertStmt(String table, List<String> columns, int parameterCount) implements Statement {}

  record SetClause(String column, int paramIndex) {}

  record UpdateStmt(String table, List<SetClause> sets, List<Condition> where, int parameterCount)
      implements Statement {}

  record DeleteStmt(String table, List<Condition> where, int parameterCount) implements Statement {}
}
