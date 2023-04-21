/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class CommandParser {
  private static final Pattern SET_PROPERTY = Pattern.compile(
      "\\s*SET\\s+'((?:[^']*|(?:''))*)'\\s*=\\s*'((?:[^']*|(?:''))*)'\\s*;\\s*",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern UNSET_PROPERTY = Pattern.compile(
      "\\s*UNSET\\s+'((?:[^']*|(?:''))*)'\\s*;\\s*", Pattern.CASE_INSENSITIVE);
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String INSERT = "INSERT";
  private static final String INTO = "INTO";
  private static final String VALUES = "VALUES";
  private static final String SELECT = "SELECT";
  private static final String CREATE = "CREATE";
  private static final String SINK = "SINK";
  private static final String SOURCE = "SOURCE";
  private static final String DROP = "DROP";
  private static final String CONNECTOR = "CONNECTOR";
  private static final String SET = "SET";
  private static final String UNSET = "UNSET";
  private static final String DEFINE = "DEFINE";
  private static final String UNDEFINE = "UNDEFINE";
  private static final String DESCRIBE = "DESCRIBE";
  private static final String EXPLAIN = "EXPLAIN";
  private static final String PRINT = "PRINT";
  private static final String SHOW = "SHOW";
  private static final String LIST = "LIST";
  private static final String RUN = "RUN";
  private static final String SCRIPT = "SCRIPT";
  private static final String SHORT_COMMENT_OPENER = "--";
  private static final String SHORT_COMMENT_CLOSER = "\n";
  private static final String LONG_COMMENT_OPENER = "/*";
  private static final String LONG_COMMENT_CLOSER = "*/";
  private static final char SINGLE_QUOTE = '\'';
  private static final char SEMICOLON = ';';
  private static final List<String> UNSUPPORTED_STATEMENTS = ImmutableList.of(
      DEFINE, UNDEFINE, DESCRIBE, EXPLAIN, SELECT, PRINT, SHOW, LIST
  );

  private enum StatementType {
    INSERT_VALUES,
    CONNECTOR,
    STATEMENT,
    SET_PROPERTY,
    UNSET_PROPERTY
  }

  private CommandParser() {
  }

  public static List<SqlCommand> parse(final String sql) {
    return splitSql(sql).stream()
        .map(CommandParser::transformToSqlCommand)
        .collect(Collectors.toList());
  }

  /**
  * Splits a string of sql commands into a list of commands and filters out the comments.
  * Note that escaped strings are not handled, because they end up getting split into two adjacent
  * strings and are pieced back together afterwards.
  *
  * @return list of commands with comments removed
  */
  @VisibleForTesting
  static List<String> splitSql(final String sql) {
    final List<String> commands = new ArrayList<>();
    StringBuffer current = new StringBuffer();
    int index = 0;
    while (index < sql.length()) {
      if (sql.charAt(index) == SINGLE_QUOTE) {
        final int closingToken = sql.indexOf(SINGLE_QUOTE, index + 1);
        validateToken(String.valueOf(SINGLE_QUOTE), closingToken);
        current.append(sql.substring(index, closingToken + 1));
        index = closingToken + 1;
      } else if (index < sql.length() - 1 && sql.startsWith(SHORT_COMMENT_OPENER, index)) {
        index = sql.indexOf(SHORT_COMMENT_CLOSER, index + 1) + 1;
        validateToken(SHORT_COMMENT_CLOSER, index - 1);
      } else if (index < sql.length() - 1 && sql.startsWith(LONG_COMMENT_OPENER, index)) {
        index = sql.indexOf(LONG_COMMENT_CLOSER, index + 1) + 2;
        validateToken(LONG_COMMENT_CLOSER, index - 2);
      } else if (sql.charAt(index) == SEMICOLON) {
        current.append(';');
        commands.add(current.toString());
        current = new StringBuffer();
        index++;
      } else {
        current.append(sql.charAt(index));
        index++;
      }
    }

    if (!current.toString().trim().isEmpty()) {
      throw new MigrationException("Unmatched command at end of file; missing semicolon: '"
          + current.toString() + "'");
    }

    return commands;
  }

  /**
  * Converts an expression into a Java object.
  */
  private static void validateToken(final String token, final int index) {
    if (index < 0) {
      throw new MigrationException("Invalid sql - failed to find closing token '" + token + "'");
    }
  }

  /**
  * Converts a generic expression into the proper Java type.
  */
  public static Object toFieldType(final Expression expressionValue) {
    if (expressionValue instanceof StringLiteral) {
      return ((StringLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof IntegerLiteral) {
      return ((IntegerLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof LongLiteral) {
      return ((LongLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof DoubleLiteral) {
      return ((DoubleLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof BooleanLiteral) {
      return ((BooleanLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof DecimalLiteral) {
      return ((DecimalLiteral) expressionValue).getValue();
    } else if (expressionValue instanceof CreateArrayExpression) {
      return ((CreateArrayExpression) expressionValue)
          .getValues()
          .stream()
          .map(CommandParser::toFieldType).collect(Collectors.toList());
    } else if (expressionValue instanceof CreateMapExpression) {
      final Map<Object, Object> resolvedMap = new HashMap<>();
      ((CreateMapExpression) expressionValue).getMap()
          .forEach((k, v) -> resolvedMap.put(toFieldType(k), toFieldType(v)));
      return resolvedMap;
    } else if (expressionValue instanceof CreateStructExpression) {
      final Map<Object, Object> resolvedStruct = new HashMap<>();
      ((CreateStructExpression) expressionValue)
          .getFields().forEach(
              field -> resolvedStruct.put(field.getName(), toFieldType(field.getValue())));
      return resolvedStruct;
    }
    throw new IllegalStateException("Expression type not recognized: "
        + expressionValue.toString());
  }

  /**
  * Determines the type of command a sql string is and returns a SqlCommand.
  */
  private static SqlCommand transformToSqlCommand(final String sql) {
    final List<String> tokens = Arrays
        .stream(sql.toUpperCase().split("\\s+"))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    switch (getStatementType(tokens)) {
      case INSERT_VALUES:
        final InsertValues parsedStatement;
        try {
          parsedStatement = (InsertValues) new AstBuilder(TypeRegistry.EMPTY)
              .buildStatement(KSQL_PARSER.parse(sql).get(0).getStatement());
        } catch (ParseFailedException e) {
          throw new MigrationException(String.format(
              "Failed to parse INSERT VALUES statement. Statement: %s. Reason: %s",
              sql, e.getMessage()));
        }
        return new SqlInsertValues(
            sql,
            parsedStatement.getTarget().text(),
            parsedStatement.getValues(),
            parsedStatement.getColumns().stream()
                .map(ColumnName::text).collect(Collectors.toList()));
      case CONNECTOR:
        return new SqlConnectorStatement(sql);
      case STATEMENT:
        return new SqlStatement(sql);
      case SET_PROPERTY:
        final Matcher setPropertyMatcher = SET_PROPERTY.matcher(sql);
        if (!setPropertyMatcher.matches()) {
          throw new MigrationException("Invalid SET command: " + sql);
        }
        return new SqlPropertyCommand(
            sql, true, setPropertyMatcher.group(1), Optional.of(setPropertyMatcher.group(2)));
      case UNSET_PROPERTY:
        final Matcher unsetPropertyMatcher = UNSET_PROPERTY.matcher(sql);
        if (!unsetPropertyMatcher.matches()) {
          throw new MigrationException("Invalid UNSET command: " + sql);
        }
        return new SqlPropertyCommand(
            sql, false, unsetPropertyMatcher.group(1), Optional.empty());
      default:
        throw new IllegalStateException();
    }
  }

  private static StatementType getStatementType(final List<String> tokens) {
    if (isInsertValuesStatement(tokens)) {
      return StatementType.INSERT_VALUES;
    } else if (isCreateConnectorStatement(tokens) || isDropConnectorStatement(tokens)) {
      return StatementType.CONNECTOR;
    } else if (tokens.size() > 0 && tokens.get(0).equals(SET)) {
      return StatementType.SET_PROPERTY;
    } else if (tokens.size() > 0 && tokens.get(0).equals(UNSET)) {
      return StatementType.UNSET_PROPERTY;
    } else {
      validateSupportedStatementType(tokens);
      return StatementType.STATEMENT;
    }
  }

  private static boolean isInsertValuesStatement(final List<String> tokens) {
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    return tokens.size() > 3
        && tokens.get(0).equals(INSERT)
        && tokens.get(1).equals(INTO)
        && !tokens.get(3).equals(SELECT)
        && tokens.contains(VALUES);
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
  }


  private static boolean isCreateConnectorStatement(final List<String> tokens) {
    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    return tokens.size() > 2
        && tokens.get(0).equals(CREATE)
        && (tokens.get(1).equals(SINK) || tokens.get(1).equals(SOURCE))
        && tokens.get(2).equals(CONNECTOR);
    // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
  }

  private static boolean isDropConnectorStatement(final List<String> tokens) {
    return tokens.size() > 1 && tokens.get(0).equals(DROP) && tokens.get(1).equals(CONNECTOR);
  }

  /**
   * Validates that the sql statement represented by the list of input tokens
   * (keywords separated whitespace, or strings identified by single quotes)
   * is not unsupported by the migrations tool. Assumes that tokens have already
   * been upper-cased.
   *
   * @param tokens components that make up the sql statement. Each component is
   *               either a keyword separated by whitespace or a string enclosed
   *               in single quotes. Assumes that tokens have already been upper-cased.
   */
  private static void validateSupportedStatementType(final List<String> tokens) {
    if (tokens.size() > 0 && UNSUPPORTED_STATEMENTS.contains(tokens.get(0))) {
      throw new MigrationException("'" + tokens.get(0) + "' statements are not supported.");
    }
    if (tokens.size() > 1 && tokens.get(0).equals(RUN) && tokens.get(1).equals(SCRIPT)) {
      throw new MigrationException("'RUN SCRIPT' statements are not supported.");
    }
  }

  public abstract static class SqlCommand {
    private final String command;

    SqlCommand(final String command) {
      this.command = command;
    }

    public String getCommand() {
      return command;
    }
  }

  /**
   * Represents ksqlDB `INSERT INTO ... VALUES ...;` statements
   */
  public static class SqlInsertValues extends SqlCommand {
    private final String sourceName;
    private final List<String> columns;
    private final List<Expression> values;

    SqlInsertValues(
        final String command,
        final String sourceName,
        final List<Expression> values,
        final List<String> columns
    ) {
      super(command);
      this.sourceName = sourceName;
      this.values = values;
      this.columns = columns;
    }

    public String getSourceName() {
      return sourceName;
    }

    public List<Expression> getValues() {
      return values;
    }

    public List<String> getColumns() {
      return columns;
    }
  }

  /**
  * Represents commands that can be sent directly to the Java client's `executeStatement` method
  */
  public static class SqlStatement extends SqlCommand {
    SqlStatement(final String command) {
      super(command);
    }
  }

  /**
   * Represents commands that deal with connectors.
   */
  public static class SqlConnectorStatement extends SqlCommand {
    SqlConnectorStatement(final String command) {
      super(command);
    }
  }

  /**
   * Represents set/unset property commands.
   */
  public static class SqlPropertyCommand extends SqlCommand {
    private final boolean set;
    private final String property;
    private final Optional<String> value;

    SqlPropertyCommand(
        final String command,
        final boolean set,
        final String property,
        final Optional<String> value
    ) {
      super(command);
      this.set = Objects.requireNonNull(set);
      this.property = Objects.requireNonNull(property);
      this.value = value;
    }

    public boolean isSetCommand() {
      return set;
    }

    public String getProperty() {
      return property;
    }

    public Optional<String> getValue() {
      return value;
    }
  }
}
