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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CommandParser {
  private static final Pattern QUOTED_STRING_OR_SEMICOLON_PATTERN =
      Pattern.compile("('([^']*|(''))*')|;", Pattern.DOTALL);
  private static final Pattern INSERT_VALUES_PATTERN = Pattern.compile(
      "\\s*INSERT\\s+INTO\\s+[\\S]+(\\s+\\(.*\\))?\\s+VALUES\\s+\\(.*\\)\\s*;\\s*",
      Pattern.DOTALL);
  private static final Pattern CREATE_CONNECTOR_PATTERN =
      Pattern.compile("\\s*CREATE\\s+(SOURCE|SINK)\\s+CONNECTOR\\s+.*;\\s*", Pattern.DOTALL);
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();

  private CommandParser() {
  }

  public static List<SqlCommand> parse(final String sql) {
    final List<String> commands = collectCommands(tokenize(sql));

    return commands.stream()
        .map(CommandParser::transformToSqlCommand)
        .collect(Collectors.toList());
  }

  /*
  * Returns the string split up by semicolons and quoted strings.
  *
  * @return a list containing the parts of the chopped up string.
  * */
  private static List<String> tokenize(final String sql) {
    final List<String> result = new ArrayList<>();
    final Matcher matcher = QUOTED_STRING_OR_SEMICOLON_PATTERN.matcher(sql);
    int prev = 0;
    while (matcher.find()) {
      result.add(sql.substring(prev, matcher.start()));
      result.add(matcher.group());
      prev = matcher.end();
    }
    result.add(sql.substring(prev));

    return result;
  }

  /*
  * Combines the list of strings returned by the tokenize method into a list of strings split by
  * semicolons.
  *
  * @return a list containing the recombined strings.
  **/
  private static List<String> collectCommands(final List<String> parts) {
    final List<String> commands = new ArrayList<>();
    String current = "";
    for (String part : parts) {
      if (part.equals(";")) {
        current += part;
        commands.add(current);
        current = "";
      } else {
        current += part;
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
    if (INSERT_VALUES_PATTERN.matcher(sql.toUpperCase()).matches()) {
      final InsertValues parsedStatement = (InsertValues) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(sql).get(0).getStatement());
      return new SqlInsertValues(
          sql,
          parsedStatement.getTarget().text(),
          parsedStatement.getValues(),
          parsedStatement.getColumns().stream()
              .map(name -> name.text()).collect(Collectors.toList()));
    } else if (CREATE_CONNECTOR_PATTERN.matcher(sql.toUpperCase()).matches()) {
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
