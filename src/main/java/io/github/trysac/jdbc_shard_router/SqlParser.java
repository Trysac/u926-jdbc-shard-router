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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Hand-written SQL tokenizer and recursive-descent parser for the supported SQL subset. */
final class SqlParser {

  static final int MAX_SQL_LENGTH = 16_384;

  private static final Set<String> FORBIDDEN_KEYWORDS =
      Set.of(
          "JOIN",
          "INNER",
          "LEFT",
          "RIGHT",
          "FULL",
          "CROSS",
          "UNION",
          "INTERSECT",
          "EXCEPT",
          "HAVING",
          "DISTINCT",
          "OR",
          "IN",
          "BETWEEN",
          "EXISTS",
          "LIKE",
          "CASE",
          "WHEN",
          "OVER",
          "WINDOW",
          "WITH");

  private static final Set<String> RESERVED_CLAUSE_KEYWORDS =
      Set.of(
          "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "INSERT", "INTO",
          "VALUES", "UPDATE", "SET", "DELETE", "AND", "BY", "ASC", "DESC");

  enum TokenType {
    IDENT,
    NUMBER,
    STAR,
    COMMA,
    LPAREN,
    RPAREN,
    QUESTION,
    OP,
    EOF
  }

  record Token(TokenType type, String text, String upper) {}

  private final List<Token> tokens;
  private int pos = 0;
  private int paramCounter = 0;

  private SqlParser(List<Token> tokens) {
    this.tokens = tokens;
  }

  static Statement parse(String sql) {
    if (sql == null) {
      throw new ShardException("unsupported_sql", "sql must not be null");
    }
    if (sql.length() > MAX_SQL_LENGTH) {
      throw new ShardException(
          "sql_too_long", "SQL length " + sql.length() + " exceeds maximum " + MAX_SQL_LENGTH);
    }
    List<Token> tokens = tokenize(sql);
    SqlParser parser = new SqlParser(tokens);
    return parser.parseStatement();
  }

  private static List<Token> tokenize(String sql) {
    List<Token> result = new ArrayList<>();
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (c == ';') {
        i++;
        while (i < n && Character.isWhitespace(sql.charAt(i))) {
          i++;
        }
        if (i < n) {
          throw new ShardException("unsupported_sql", "multiple SQL statements are not allowed");
        }
        break;
      }
      if (c == '*') {
        result.add(new Token(TokenType.STAR, "*", "*"));
        i++;
      } else if (c == ',') {
        result.add(new Token(TokenType.COMMA, ",", ","));
        i++;
      } else if (c == '(') {
        result.add(new Token(TokenType.LPAREN, "(", "("));
        i++;
      } else if (c == ')') {
        result.add(new Token(TokenType.RPAREN, ")", ")"));
        i++;
      } else if (c == '?') {
        result.add(new Token(TokenType.QUESTION, "?", "?"));
        i++;
      } else if (c == '=') {
        result.add(new Token(TokenType.OP, "=", "="));
        i++;
      } else if (c == '<') {
        if (i + 1 < n && sql.charAt(i + 1) == '=') {
          result.add(new Token(TokenType.OP, "<=", "<="));
          i += 2;
        } else if (i + 1 < n && sql.charAt(i + 1) == '>') {
          result.add(new Token(TokenType.OP, "<>", "<>"));
          i += 2;
        } else {
          result.add(new Token(TokenType.OP, "<", "<"));
          i++;
        }
      } else if (c == '>') {
        if (i + 1 < n && sql.charAt(i + 1) == '=') {
          result.add(new Token(TokenType.OP, ">=", ">="));
          i += 2;
        } else {
          result.add(new Token(TokenType.OP, ">", ">"));
          i++;
        }
      } else if (c == '!') {
        if (i + 1 < n && sql.charAt(i + 1) == '=') {
          result.add(new Token(TokenType.OP, "!=", "!="));
          i += 2;
        } else {
          throw new ShardException("unsupported_sql", "unexpected character '!'");
        }
      } else if (Character.isDigit(c)) {
        int start = i;
        while (i < n && Character.isDigit(sql.charAt(i))) {
          i++;
        }
        String num = sql.substring(start, i);
        result.add(new Token(TokenType.NUMBER, num, num));
      } else if (Character.isLetter(c) || c == '_') {
        int start = i;
        while (i < n && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) {
          i++;
        }
        String ident = sql.substring(start, i);
        String upper = ident.toUpperCase(Locale.ROOT);
        if (FORBIDDEN_KEYWORDS.contains(upper)) {
          throw new ShardException("unsupported_sql", "unsupported SQL keyword: " + upper);
        }
        result.add(new Token(TokenType.IDENT, ident, upper));
      } else {
        throw new ShardException("unsupported_sql", "unsupported SQL syntax or literal");
      }
    }
    result.add(new Token(TokenType.EOF, "", ""));
    return result;
  }

  private Statement parseStatement() {
    Token first = peek();
    if (first.type() != TokenType.IDENT) {
      throw new ShardException("unsupported_sql", "expected statement keyword");
    }
    Statement stmt =
        switch (first.upper()) {
          case "SELECT" -> parseSelect();
          case "INSERT" -> parseInsert();
          case "UPDATE" -> parseUpdate();
          case "DELETE" -> parseDelete();
          default ->
              throw new ShardException(
                  "unsupported_sql", "unsupported statement type: " + first.upper());
        };
    if (peek().type() != TokenType.EOF) {
      throw new ShardException("unsupported_sql", "unexpected trailing tokens in SQL");
    }
    return stmt;
  }

  private SelectStmt parseSelect() {
    expectKeyword("SELECT");
    List<SelectItem> items = new ArrayList<>();
    items.add(parseSelectItem());
    while (match(TokenType.COMMA)) {
      items.add(parseSelectItem());
    }
    expectKeyword("FROM");
    String table = parseIdentifier();
    if (peek().type() == TokenType.IDENT && !RESERVED_CLAUSE_KEYWORDS.contains(peek().upper())) {
      throw new ShardException("unsupported_sql", "table aliases or joins are not supported");
    }
    if (peek().type() == TokenType.COMMA) {
      throw new ShardException("unsupported_sql", "comma joins are not supported");
    }

    List<Condition> where = List.of();
    if (matchKeyword("WHERE")) {
      where = parseWhere();
    }

    List<String> groupBy = List.of();
    if (matchKeyword("GROUP")) {
      expectKeyword("BY");
      groupBy = new ArrayList<>();
      groupBy.add(parseIdentifier());
      while (match(TokenType.COMMA)) {
        groupBy.add(parseIdentifier());
      }
    }

    List<OrderSpec> orderBy = List.of();
    if (matchKeyword("ORDER")) {
      expectKeyword("BY");
      orderBy = new ArrayList<>();
      orderBy.add(parseOrderSpec());
      while (match(TokenType.COMMA)) {
        orderBy.add(parseOrderSpec());
      }
    }

    Integer limit = null;
    Integer offset = null;
    if (matchKeyword("LIMIT")) {
      limit = parseNonNegativeInt();
      if (matchKeyword("OFFSET")) {
        offset = parseNonNegativeInt();
      }
    } else if (matchKeyword("OFFSET")) {
      throw new ShardException("unsupported_sql", "OFFSET without LIMIT is not supported");
    }

    validateSelectSemantics(items, groupBy);

    return new SelectStmt(items, table, where, groupBy, orderBy, limit, offset, paramCounter);
  }

  private void validateSelectSemantics(List<SelectItem> items, List<String> groupBy) {
    boolean hasAgg = false;
    boolean hasCol = false;
    for (SelectItem item : items) {
      if (item instanceof AggregateItem) {
        hasAgg = true;
      } else if (item instanceof ColumnItem) {
        hasCol = true;
      }
    }
    if (groupBy.isEmpty() && hasAgg && hasCol) {
      throw new ShardException(
          "unsupported_sql", "mixing scalar columns and aggregates without GROUP BY");
    }
    if (!groupBy.isEmpty()) {
      for (SelectItem item : items) {
        if (item instanceof ColumnItem col) {
          boolean found = groupBy.stream().anyMatch(g -> g.equalsIgnoreCase(col.column()));
          if (!found) {
            throw new ShardException(
                "unsupported_sql", "selected column " + col.column() + " must appear in GROUP BY");
          }
        }
      }
    }
  }

  private SelectItem parseSelectItem() {
    Token t = peek();
    if (t.type() != TokenType.IDENT) {
      throw new ShardException("unsupported_sql", "expected column or aggregate in SELECT");
    }
    String upper = t.upper();
    if (upper.equals("COUNT")
        || upper.equals("SUM")
        || upper.equals("MIN")
        || upper.equals("MAX")
        || upper.equals("AVG")) {
      advance();
      expect(TokenType.LPAREN);
      AggFunc func = AggFunc.valueOf(upper);
      String col;
      if (func == AggFunc.COUNT && match(TokenType.STAR)) {
        col = "*";
      } else {
        col = parseIdentifier();
      }
      expect(TokenType.RPAREN);
      return new AggregateItem(func, col);
    }
    String col = parseIdentifier();
    if (peek().type() == TokenType.LPAREN) {
      throw new ShardException("unsupported_sql", "unsupported function in SELECT: " + col);
    }
    return new ColumnItem(col);
  }

  private OrderSpec parseOrderSpec() {
    String col = parseIdentifier();
    boolean asc = true;
    if (matchKeyword("ASC")) {
      asc = true;
    } else if (matchKeyword("DESC")) {
      asc = false;
    }
    return new OrderSpec(col, asc);
  }

  private InsertStmt parseInsert() {
    expectKeyword("INSERT");
    expectKeyword("INTO");
    String table = parseIdentifier();
    expect(TokenType.LPAREN);
    List<String> cols = new ArrayList<>();
    cols.add(parseIdentifier());
    while (match(TokenType.COMMA)) {
      cols.add(parseIdentifier());
    }
    expect(TokenType.RPAREN);
    expectKeyword("VALUES");
    expect(TokenType.LPAREN);
    expectQuestion();
    int placeholders = 1;
    while (match(TokenType.COMMA)) {
      expectQuestion();
      placeholders++;
    }
    expect(TokenType.RPAREN);
    if (placeholders != cols.size()) {
      throw new ShardException(
          "unsupported_sql", "INSERT column count does not match VALUES placeholder count");
    }
    return new InsertStmt(table, List.copyOf(cols), paramCounter);
  }

  private UpdateStmt parseUpdate() {
    expectKeyword("UPDATE");
    String table = parseIdentifier();
    expectKeyword("SET");
    List<SetClause> sets = new ArrayList<>();
    sets.add(parseSetClause());
    while (match(TokenType.COMMA)) {
      sets.add(parseSetClause());
    }
    List<Condition> where = List.of();
    if (matchKeyword("WHERE")) {
      where = parseWhere();
    }
    return new UpdateStmt(table, List.copyOf(sets), where, paramCounter);
  }

  private SetClause parseSetClause() {
    String col = parseIdentifier();
    Token op = expect(TokenType.OP);
    if (!op.text().equals("=")) {
      throw new ShardException("unsupported_sql", "expected '=' in SET clause");
    }
    int idx = expectQuestion();
    return new SetClause(col, idx);
  }

  private DeleteStmt parseDelete() {
    expectKeyword("DELETE");
    expectKeyword("FROM");
    String table = parseIdentifier();
    List<Condition> where = List.of();
    if (matchKeyword("WHERE")) {
      where = parseWhere();
    }
    return new DeleteStmt(table, where, paramCounter);
  }

  private List<Condition> parseWhere() {
    List<Condition> conds = new ArrayList<>();
    conds.add(parseCondition());
    while (matchKeyword("AND")) {
      conds.add(parseCondition());
    }
    return List.copyOf(conds);
  }

  private Condition parseCondition() {
    if (peek().type() == TokenType.LPAREN) {
      throw new ShardException("unsupported_sql", "subqueries or parenthesized WHERE not allowed");
    }
    String col = parseIdentifier();
    Token op = expect(TokenType.OP);
    int idx = expectQuestion();
    return new Condition(col, op.text(), idx);
  }

  private int expectQuestion() {
    if (peek().type() != TokenType.QUESTION) {
      throw new ShardException("unsupported_sql", "only '?' parameters are allowed in expressions");
    }
    advance();
    return paramCounter++;
  }

  private int parseNonNegativeInt() {
    Token t = expect(TokenType.NUMBER);
    try {
      return Integer.parseInt(t.text());
    } catch (NumberFormatException e) {
      throw new ShardException("unsupported_sql", "invalid integer literal in LIMIT/OFFSET");
    }
  }

  private String parseIdentifier() {
    Token t = peek();
    if (t.type() != TokenType.IDENT || RESERVED_CLAUSE_KEYWORDS.contains(t.upper())) {
      throw new ShardException("unsupported_sql", "expected identifier");
    }
    advance();
    return t.text();
  }

  private void expectKeyword(String keyword) {
    Token t = peek();
    if (t.type() != TokenType.IDENT || !t.upper().equals(keyword)) {
      throw new ShardException("unsupported_sql", "expected keyword " + keyword);
    }
    advance();
  }

  private boolean matchKeyword(String keyword) {
    Token t = peek();
    if (t.type() == TokenType.IDENT && t.upper().equals(keyword)) {
      advance();
      return true;
    }
    return false;
  }

  private Token expect(TokenType type) {
    Token t = peek();
    if (t.type() != type) {
      throw new ShardException("unsupported_sql", "expected token " + type);
    }
    advance();
    return t;
  }

  private boolean match(TokenType type) {
    if (peek().type() == type) {
      advance();
      return true;
    }
    return false;
  }

  private Token peek() {
    return tokens.get(pos);
  }

  private void advance() {
    if (pos < tokens.size() - 1) {
      pos++;
    }
  }
}
