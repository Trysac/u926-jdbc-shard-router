package io.github.trysac.jdbc_shard_router;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable tabular result of a routed {@code SELECT} query.
 *
 * @param columns the column names in projection order
 * @param rows the rows, where each row is an unmodifiable list of column values (which may contain
 *     {@code null})
 */
public record ResultTable(List<String> columns, List<List<Object>> rows) {

  /** Creates an immutable {@code ResultTable}. */
  public ResultTable {
    Objects.requireNonNull(columns, "columns");
    Objects.requireNonNull(rows, "rows");
    columns = List.copyOf(columns);
    List<List<Object>> copiedRows = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      Objects.requireNonNull(row, "row");
      copiedRows.add(Collections.unmodifiableList(new ArrayList<>(row)));
    }
    rows = Collections.unmodifiableList(copiedRows);
  }
}
