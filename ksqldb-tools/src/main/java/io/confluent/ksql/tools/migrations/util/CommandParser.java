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
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import io.confluent.ksql.metastore.TypeRegistryImpl;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.VariableSubstitutor;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class CommandParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final String QUOTED_STRING_OR_WHITESPACE = "('([^']*|(''))*')|\\s+";
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

  private enum StatementType {
    INSERT_VALUES(InsertValues.class),
    CREATE_CONNECTOR(CreateConnector.class),
    DROP_CONNECTOR(DropConnector.class),
    STATEMENT(Statement.class),
    SET_PROPERTY(SetProperty.class),
    UNSET_PROPERTY(UnsetProperty.class),
    DEFINE_VARIABLE(DefineVariable.class),
    UNDEFINE_VARIABLE(UndefineVariable.class),
    ASSERT_TOPIC(AssertTopic.class),
    ASSERT_SCHEMA(AssertSchema.class);

    private final Class<? extends Statement> statementClass;

    <T extends Statement> StatementType(
            final Class<T> statementClass) {
      this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
    }

    public static StatementType get(Class<? extends Statement> statementClass) {
      Optional<StatementType> type = Arrays.stream(StatementType.values())
              .filter(statementType -> statementType.statementClass.equals(statementClass))
              .findFirst();
      return type.orElse(StatementType.STATEMENT);
    }
  }

  private CommandParser() {
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

  /**
  * Determines the type of command a sql string is and returns a SqlCommand.
  */
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  public static SqlCommand transformToSqlCommand(
          // CHECKSTYLE_RULES.ON: CyclomaticComplexity
          final String sql, final Map<String, String> variables) {

    validateSupportedStatementType(sql);
    final String substituted;
    try {
      substituted = VariableSubstitutor.substitute( KSQL_PARSER.parse(sql).get(0), variables);
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
          "Failed to parse the statement. Statement: %s. Reason: %s",
          sql, e.getMessage()));
    }
    final Class<? extends Statement> statementClass = KSQL_PARSER.prepare
            (KSQL_PARSER.parse(substituted).get(0), TypeRegistry.EMPTY).getStatement().getClass();

    switch (StatementType.get(statementClass)) {
      case INSERT_VALUES:
        return getInsertValuesStatement(substituted);
      case CREATE_CONNECTOR:
        return getCreateConnectorStatement(substituted);
      case DROP_CONNECTOR:
        return getDropConnectorStatement(substituted);
      case STATEMENT:
        return new SqlStatement(sql);
      case SET_PROPERTY:
        return getSetPropertyCommand(substituted);
      case UNSET_PROPERTY:
        return getUnsetPropertyCommand(substituted);
      case DEFINE_VARIABLE:
        return getDefineVariableCommand(substituted);
      case UNDEFINE_VARIABLE:
        return getUndefineVariableCommand(sql);
      case ASSERT_SCHEMA:
        return getAssertSchemaCommand(substituted);
      case ASSERT_TOPIC:
        return getAssertTopicCommand(substituted);
      default:
        throw new IllegalStateException();
    }
  }

  private static SqlInsertValues getInsertValuesStatement(final String substituted) {
    final InsertValues parsedStatement = (InsertValues) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlInsertValues(
        substituted,
        preserveCase(parsedStatement.getTarget().text()),
        parsedStatement.getValues(),
        parsedStatement.getColumns().stream()
            .map(ColumnName::text).collect(Collectors.toList()));
  }

  private static SqlCreateConnectorStatement getCreateConnectorStatement(final String substituted) {
    final CreateConnector createConnector = (CreateConnector) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlCreateConnectorStatement(
            substituted,
            preserveCase(createConnector.getName()),
            createConnector.getType() == Type.SOURCE,
            createConnector.getConfig().entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> toFieldType(e.getValue()))),
            createConnector.ifNotExists()
    );
  }

  private static SqlDropConnectorStatement getDropConnectorStatement(final String substituted) {
    final DropConnector dropConnector = (DropConnector) new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlDropConnectorStatement(
            substituted,
            preserveCase(dropConnector.getConnectorName()),
            dropConnector.getIfExists()
    );
  }

  private static SqlPropertyCommand getSetPropertyCommand(final String substituted) {
    final SetProperty setProperty = (SetProperty) new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlPropertyCommand(
        substituted,
        true,
        setProperty.getPropertyName(),
        Optional.of(setProperty.getPropertyValue())
    );
  }

  private static SqlPropertyCommand getUnsetPropertyCommand(final String substituted) {
    final UnsetProperty unsetProperty = (UnsetProperty) new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlPropertyCommand(
        substituted,
        false,
        unsetProperty.getPropertyName(),
        Optional.empty());
  }

  private static SqlDefineVariableCommand getDefineVariableCommand(final String substituted) {
    final DefineVariable defineVariable = (DefineVariable) new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlDefineVariableCommand(
            substituted,
            defineVariable.getVariableName(),
            defineVariable.getVariableValue()
    );
  }

  private static SqlUndefineVariableCommand getUndefineVariableCommand(final String substituted) {
    final UndefineVariable undefineVariable = (UndefineVariable) new AstBuilder(TypeRegistry.EMPTY)
            .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlUndefineVariableCommand(substituted, undefineVariable.getVariableName());
  }

  private static SqlAssertSchemaCommand getAssertSchemaCommand(final String substituted) {
    final AssertSchema assertSchema = (AssertSchema) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    return new SqlAssertSchemaCommand(
        substituted,
        assertSchema.getSubject(),
        assertSchema.getId(),
        assertSchema.checkExists(),
        assertSchema.getTimeout().isPresent()
            ? Optional.of(assertSchema.getTimeout().get().toDuration())
            : Optional.empty()
    );
  }

  private static SqlAssertTopicCommand getAssertTopicCommand(final String substituted) {
    final AssertTopic assertTopic = (AssertTopic) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    final Map<String, Integer> configs = assertTopic.getConfig().entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> (Integer) e.getValue().getValue()));
    return new SqlAssertTopicCommand(
        substituted,
        assertTopic.getTopic(),
        configs,
        assertTopic.checkExists(),
        assertTopic.getTimeout().isPresent()
            ? Optional.of(assertTopic.getTimeout().get().toDuration())
            : Optional.empty()
    );
  }


  /**
   * Validates that the sql statement represented by the list of input tokens
   * (keywords separated whitespace, or strings identified by single quotes)
   * is not unsupported by the migrations tool. Assumes that tokens have already
   * been upper-cased.
   *
   * @param sql components that make up the sql statement. Each component is
   *               either a keyword separated by whitespace or a string enclosed
   *               in single quotes. Assumes that tokens have already been upper-cased.
   */
  private static void validateSupportedStatementType(final String sql) {
    final List<String> tokens = Arrays
            .stream(sql.toUpperCase().split(QUOTED_STRING_OR_WHITESPACE))
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
    private final ImmutableList<String> columns;
    private final ImmutableList<Expression> values;

    SqlInsertValues(
        final String command,
        final String sourceName,
        final List<Expression> values,
        final List<String> columns
    ) {
      super(command);
      this.sourceName = sourceName;
      this.values = ImmutableList.copyOf(values);
      this.columns = ImmutableList.copyOf(columns);
    }

    public String getSourceName() {
      return sourceName;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "values is ImmutableList")
    public List<Expression> getValues() {
      return values;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columns is ImmutableList")
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
   * Represents ksqlDB CREATE CONNECTOR statements.
   */
  public static class SqlCreateConnectorStatement extends SqlCommand {
    final String name;
    final boolean isSource;
    final ImmutableMap<String, Object> properties;
    final boolean ifNotExist;

    SqlCreateConnectorStatement(
        final String command,
        final String name,
        final boolean isSource,
        final Map<String, Object> properties,
        final boolean ifNotExist
    ) {
      super(command);
      this.name = Objects.requireNonNull(name);
      this.isSource = isSource;
      this.properties = ImmutableMap.copyOf(Objects.requireNonNull(properties));
      this.ifNotExist = ifNotExist;
    }

    public String getName() {
      return name;
    }

    public boolean isSource() {
      return isSource;
    }

    public boolean getIfNotExists() {
      return ifNotExist;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "properties is ImmutableMap")
    public Map<String, Object> getProperties() {
      return properties;
    }
  }

  /**
   * Represents ksqlDB DROP CONNECTOR statements.
   */
  public static class SqlDropConnectorStatement extends SqlCommand {
    final String name;
    final boolean ifExists;

    SqlDropConnectorStatement(
        final String command,
        final String name,
        final boolean ifExists
    ) {
      super(command);
      this.name = Objects.requireNonNull(name);
      this.ifExists = ifExists;
    }

    public String getName() {
      return name;
    }

    public boolean getIfExists() {
      return ifExists;
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
      this.set = set;
      this.property = Objects.requireNonNull(property);
      this.value = Objects.requireNonNull(value);
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

  /**
   * Represents ksqlDB DEFINE commands.
   */
  public static class SqlDefineVariableCommand extends SqlCommand {
    private final String variable;
    private final String value;

    SqlDefineVariableCommand(
        final String command,
        final String variable,
        final String value
    ) {
      super(command);
      this.variable = Objects.requireNonNull(variable);
      this.value = Objects.requireNonNull(value);
    }

    public String getVariable() {
      return variable;
    }

    public String getValue() {
      return value;
    }
  }

  /**
   * Represents ksqlDB UNDEFINE commands.
   */
  public static class SqlUndefineVariableCommand extends SqlCommand {
    private final String variable;

    SqlUndefineVariableCommand(
        final String command,
        final String variable
    ) {
      super(command);
      this.variable = Objects.requireNonNull(variable);
    }

    public String getVariable() {
      return variable;
    }
  }

  /**
   * Represents ksqlDB ASSERT TOPIC commands.
   */
  public static class SqlAssertTopicCommand extends SqlCommand {
    private final String topic;
    private final ImmutableMap<String, Integer> configs;
    private final boolean exists;
    private final Optional<Duration> timeout;

    SqlAssertTopicCommand(
        final String command,
        final String topic,
        final Map<String, Integer> configs,
        final boolean exists,
        final Optional<Duration> timeout
    ) {
      super(command);
      this.topic = Objects.requireNonNull(topic);
      this.configs = ImmutableMap.copyOf(Objects.requireNonNull(configs));
      this.exists = exists;
      this.timeout = Objects.requireNonNull(timeout);
    }

    public String getTopic() {
      return topic;
    }

    public Map<String, Integer> getConfigs() {
      return configs;
    }

    public boolean getExists() {
      return exists;
    }

    public Optional<Duration> getTimeout() {
      return timeout;
    }
  }

  /**
   * Represents ksqlDB ASSERT SCHEMA commands.
   */
  public static class SqlAssertSchemaCommand extends SqlCommand {
    private final Optional<String> subject;
    private final Optional<Integer> id;
    private final boolean exists;
    private final Optional<Duration> timeout;

    SqlAssertSchemaCommand(
        final String command,
        final Optional<String> subject,
        final Optional<Integer> id,
        final boolean exists,
        final Optional<Duration> timeout
    ) {
      super(command);
      this.subject = Objects.requireNonNull(subject);
      this.id = Objects.requireNonNull(id);
      this.exists = exists;
      this.timeout = Objects.requireNonNull(timeout);
    }

    public Optional<String> getSubject() {
      return subject;
    }

    public Optional<Integer> getId() {
      return id;
    }

    public boolean getExists() {
      return exists;
    }

    public Optional<Duration> getTimeout() {
      return timeout;
    }
  }
}
