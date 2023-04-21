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
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.VariableSubstitutor;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class CommandParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String SELECT = "SELECT";
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
          DESCRIBE, EXPLAIN, SELECT, PRINT, SHOW, LIST
  );


  private CommandParser() {
  }

  private enum StatementType {
    INSERT_VALUES(SqlBaseParser.InsertValuesContext.class),
    CREATE_CONNECTOR(SqlBaseParser.CreateConnectorContext.class),
    DROP_CONNECTOR(SqlBaseParser.DropConnectorContext.class),
    STATEMENT(SqlBaseParser.StatementContext.class),
    SET_PROPERTY(SqlBaseParser.SetPropertyContext.class),
    UNSET_PROPERTY(SqlBaseParser.UnsetPropertyContext.class),
    DEFINE_VARIABLE(SqlBaseParser.DefineVariableContext.class),
    UNDEFINE_VARIABLE(SqlBaseParser.UndefineVariableContext.class),
    ASSERT_TOPIC(SqlBaseParser.AssertTopicContext.class),
    ASSERT_SCHEMA(SqlBaseParser.AssertSchemaContext.class);

    private final Class<? extends SqlBaseParser.StatementContext> statementClass;

    <T extends SqlBaseParser.StatementContext> StatementType(final Class<T> statementClass) {
      this.statementClass = Objects.requireNonNull(statementClass, "statementType");
    }

    public static StatementType get(
            final Class<? extends SqlBaseParser.StatementContext> statementClass) {
      final Optional<StatementType> type = Arrays.stream(StatementType.values())
              .filter(statementType -> statementType.statementClass.equals(statementClass))
              .findFirst();

      return type.orElse(StatementType.STATEMENT);
    }
  }


  /**
   * Splits a string of sql commands into a list of commands and filters out the comments.
   * Note that escaped strings are not handled, because they end up getting split into two adjacent
   * strings and are pieced back together afterwards.
   *
   * @return list of commands with comments removed
   */
  public static List<String> splitSql(final String sql) {
    final List<String> commands = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int index = 0;
    while (index < sql.length()) {
      if (sql.charAt(index) == SINGLE_QUOTE) {
        final int closingToken = sql.indexOf(SINGLE_QUOTE, index + 1);
        validateToken(String.valueOf(SINGLE_QUOTE), closingToken);
        current.append(sql, index, closingToken + 1);
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
        current = new StringBuilder();
        index++;
      } else {
        current.append(sql.charAt(index));
        index++;
      }
    }

    if (!current.toString().trim().isEmpty()) {
      throw new MigrationException(String.format(
              "Unmatched command at end of file; missing semicolon: '%s'", current
      ));
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
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static Object toFieldType(final Expression expressionValue) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
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
    } else if (expressionValue instanceof NullLiteral) {
      return null;
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


  public static ParsedCommand parse(
          // CHECKSTYLE_RULES.ON: CyclomaticComplexity
          final String sql, final Map<String, String> variables) {

    validateSupportedStatementType(sql);
    final String substituted;
    try {
      substituted = VariableSubstitutor.substitute(KSQL_PARSER.parse(sql).get(0), variables);
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
              "Failed to parse the statement. Statement: %s. Reason: %s",
              sql, e.getMessage()));
    }
    final SqlBaseParser.SingleStatementContext statementContext = KSQL_PARSER.parse(substituted)
            .get(0).getStatement();
    final boolean isStatement = StatementType.get(statementContext.statement().getClass())
            == StatementType.STATEMENT;
    return new ParsedCommand(substituted,
            isStatement ? Optional.empty() : Optional.of(new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(statementContext)));
  }


  /**
   * Validates that the sql statement represented by the list of input tokens
   * (keywords separated whitespace, or strings identified by single quotes)
   * is not unsupported by the migrations tool.
   * <p>
   * NOTE: this method is obsolete and should be replaced with type-checking after the parsing
   * step. See https://github.com/confluentinc/ksql/pull/9881#discussion_r1174178200 for more
   * details.
   * </p>
   *
   * @param sql components that make up the sql statement. Each component is
   *               either a keyword separated by whitespace or a string enclosed
   *               in single quotes. Assumes that tokens have already been upper-cased.
   */
  private static void validateSupportedStatementType(final String sql) {
    final List<String> tokens = Arrays
            .stream(sql.toUpperCase().split("\\s+"))
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    if (tokens.size() > 0 && UNSUPPORTED_STATEMENTS.contains(tokens.get(0))) {
      throw new MigrationException("'" + tokens.get(0) + "' statements are not supported.");
    }
    if (tokens.size() > 1 && tokens.get(0).equals(RUN) && tokens.get(1).equals(SCRIPT)) {
      throw new MigrationException("'RUN SCRIPT' statements are not supported.");
    }
  }

  public static String preserveCase(final String fieldOrSourceName) {
    return "`" + fieldOrSourceName + "`";
  }

  public static class ParsedCommand {
    private final String command;
    private final Optional<Statement> statement;

    ParsedCommand(final String command, final Optional<Statement> statement) {
      this.command = command;
      this.statement = statement;
    }

    public String getCommand() {
      return command;
    }

    public Optional<Statement> getStatement() {
      return statement;
    }
  }
}