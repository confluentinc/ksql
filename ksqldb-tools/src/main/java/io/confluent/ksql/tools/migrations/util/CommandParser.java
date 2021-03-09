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
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.InsertValues;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class CommandParser {
  private static final String QUOTED_STRING_OR_WHITESPACE = "('([^']*|(''))*')|\\s+";
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String INSERT = "INSERT";
  private static final String INTO = "INTO";
  private static final String VALUES = "VALUES";
  private static final String SELECT = "SELECT";
  private static final String CREATE = "CREATE";
  private static final String SINK = "SINK";
  private static final String SOURCE = "SOURCE";
  private static final String CONNECTOR = "CONNECTOR";
  private static final String SHORT_COMMENT_OPENER = "--";
  private static final String SHORT_COMMENT_CLOSER = "\n";
  private static final String LONG_COMMENT_OPENER = "/*";
  private static final String LONG_COMMENT_CLOSER = "*/";
  private static final char SINGLE_QUOTE = '\'';
  private static final char SEMICOLON = ';';

  private CommandParser() {
  }

  public static List<SqlCommand> parse(final String sql) {
    return splitSql(sql).stream()
        .map(CommandParser::transformToSqlCommand)
        .collect(Collectors.toList());
  }

  /*
  * Splits a string of sql commands into a list of commands and filters out the comments.
  *
  * @return a list of strings
  * */
  @VisibleForTesting
  static List<String> splitSql(final String sql) {
    final List<String> commands = new ArrayList<>();
    String current = "";
    for (int i = 0; i < sql.length(); i++) {
      if (sql.charAt(i) == SINGLE_QUOTE) {
        final int closingToken = sql.indexOf(SINGLE_QUOTE, i + 1);
        current += sql.substring(i, closingToken + 1);
        i = closingToken;
      } else if (i < sql.length() - 1 && sql.startsWith(SHORT_COMMENT_OPENER, i)) {
        i = sql.indexOf(SHORT_COMMENT_CLOSER, i + 1);
      } else if (i < sql.length() - 1 && sql.startsWith(LONG_COMMENT_OPENER, i)) {
        i = sql.indexOf(LONG_COMMENT_CLOSER, i + 1) + 1;
      } else if (sql.charAt(i) == SEMICOLON) {
        current += ";";
        commands.add(current);
        current = "";
      } else {
        current += sql.charAt(i);
      }
    }

    return commands;
  }

  /*
  * Converts an expression into a Java object.
  **/
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
          .map(val -> toFieldType(val)).collect(Collectors.toList());
    } else if (expressionValue instanceof CreateMapExpression) {
      final Map<Object, Object> resolvedMap = new HashMap<>();
      ((CreateMapExpression) expressionValue).getMap()
          .forEach((k, v) -> resolvedMap.put(toFieldType(k), toFieldType(v)));
      return resolvedMap;
    } else if (expressionValue instanceof CreateStructExpression) {
      final Map<Object, Object> resolvedStruct = new HashMap<>();
      ((CreateStructExpression) expressionValue)
          .getFields().stream().forEach(
              field -> resolvedStruct.put(field.getName(), toFieldType(field.getValue())));
      return resolvedStruct;
    }
    throw new IllegalStateException("Expression type not recognized: "
        + expressionValue.toString());
  }

  /*
  * Determines the type of command a sql string is and returns a SqlCommand.
  **/
  private static SqlCommand transformToSqlCommand(final String sql) {
    final List<String> tokens = Arrays
        .stream(sql.toUpperCase().split(QUOTED_STRING_OR_WHITESPACE))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    if (tokens.get(0).equals(INSERT)
        && tokens.get(1).equals(INTO)
        && !tokens.get(3).equals(SELECT)
        && tokens.contains(VALUES)
    ) {
      final InsertValues parsedStatement = (InsertValues) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(sql).get(0).getStatement());
      return new SqlInsertValues(
          sql,
          parsedStatement.getTarget().text(),
          parsedStatement.getValues(),
          parsedStatement.getColumns().stream()
              .map(name -> name.text()).collect(Collectors.toList()));
    } else if (tokens.get(0).equals(CREATE)
        && (tokens.get(1).equals(SINK) || tokens.get(1).equals(SOURCE))
        && tokens.get(2).equals(CONNECTOR)
    ) {
      return new SqlConnectorStatement(sql);
    } else {
      return new SqlStatement(sql);
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

  /*
   * Represents ksqlDb `INSERT INTO ... VALUES ...;` statements
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

  /*
  * Represents commands that can be sent directly to the the Java client's `executeStatement` method
  * */
  public static class SqlStatement extends SqlCommand {
    SqlStatement(final String command) {
      super(command);
    }
  }

  /*
   * Represents commands that deal with connectors.
   * */
  public static class SqlConnectorStatement extends SqlCommand {
    SqlConnectorStatement(final String command) {
      super(command);
    }
  }
}
