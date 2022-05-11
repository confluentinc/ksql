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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.VariableSubstitutor;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class CommandParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final String QUOTED_STRING_OR_WHITESPACE = "('([^']*|(''))*')|\\s+";
  private static final Pattern SET_PROPERTY = Pattern.compile(
      "\\s*SET\\s+'((?:[^']*|(?:''))*)'\\s*=\\s*'((?:[^']*|(?:''))*)'\\s*;\\s*",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern UNSET_PROPERTY = Pattern.compile(
      "\\s*UNSET\\s+'((?:[^']*|(?:''))*)'\\s*;\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern DROP_CONNECTOR = Pattern.compile(
      "\\s*DROP\\s+CONNECTOR\\s+(IF\\s+EXISTS\\s+)?(.*?)\\s*;\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern DEFINE_VARIABLE = Pattern.compile(
      "\\s*DEFINE\\s+([a-zA-Z_][a-zA-Z0-9_@]*)\\s*=\\s*'((?:[^']*|(?:''))*)'\\s*;\\s*",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern UNDEFINE_VARIABLE = Pattern.compile(
      "\\s*UNDEFINE\\s+([a-zA-Z_][a-zA-Z0-9_@]*)\\s*;\\s*", Pattern.CASE_INSENSITIVE);
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String ASSERT = "ASSERT";
  private static final String TOPIC = "TOPIC";
  private static final String SCHEMA = "SCHEMA";
  private static final String NOT = "NOT";
  private static final String EXISTS = "EXISTS";
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
      DESCRIBE, EXPLAIN, SELECT, PRINT, SHOW, LIST
  );

  private enum StatementType {
    INSERT_VALUES,
    CREATE_CONNECTOR,
    DROP_CONNECTOR,
    STATEMENT,
    SET_PROPERTY,
    UNSET_PROPERTY,
    DEFINE_VARIABLE,
    UNDEFINE_VARIABLE,
    ASSERT_TOPIC,
    ASSERT_SCHEMA
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

    // Splits the sql string into tokens(uppercased keyowords, identifiers and strings)
    final List<String> tokens = Arrays
        .stream(sql.toUpperCase().split(QUOTED_STRING_OR_WHITESPACE))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    switch (getStatementType(tokens)) {
      case INSERT_VALUES:
        return getInsertValuesStatement(sql, variables);
      case CREATE_CONNECTOR:
        return getCreateConnectorStatement(sql, variables);
      case DROP_CONNECTOR:
        return getDropConnectorStatement(sql);
      case STATEMENT:
        return new SqlStatement(sql);
      case SET_PROPERTY:
        return getSetPropertyCommand(sql, variables);
      case UNSET_PROPERTY:
        return getUnsetPropertyCommand(sql, variables);
      case DEFINE_VARIABLE:
        return getDefineVariableCommand(sql, variables);
      case UNDEFINE_VARIABLE:
        return getUndefineVariableCommand(sql);
      case ASSERT_SCHEMA:
        return getAssertSchemaCommand(sql, variables);
      case ASSERT_TOPIC:
        return getAssertTopicCommand(sql, variables);
      default:
        throw new IllegalStateException();
    }
  }

  private static SqlInsertValues getInsertValuesStatement(
      final String sql, final Map<String, String> variables) {
    final InsertValues parsedStatement;
    try {
      final String substituted = VariableSubstitutor.substitute(
          KSQL_PARSER.parse(sql).get(0),
          variables
      );
      parsedStatement = (InsertValues) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
          "Failed to parse INSERT VALUES statement. Statement: %s. Reason: %s",
          sql, e.getMessage()));
    }
    return new SqlInsertValues(
        sql,
        preserveCase(parsedStatement.getTarget().text()),
        parsedStatement.getValues(),
        parsedStatement.getColumns().stream()
            .map(ColumnName::text).collect(Collectors.toList()));
  }

  private static SqlCreateConnectorStatement getCreateConnectorStatement(
      final String sql,
      final Map<String, String> variables
  ) {
    final CreateConnector createConnector;
    try {
      final String substituted = VariableSubstitutor.substitute(
          KSQL_PARSER.parse(sql).get(0),
          variables
      );
      createConnector = (CreateConnector) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
          "Failed to parse CREATE CONNECTOR statement. Statement: %s. Reason: %s",
          sql, e.getMessage()));
    }
    return new SqlCreateConnectorStatement(
        sql,
        preserveCase(createConnector.getName()),
        createConnector.getType() == Type.SOURCE,
        createConnector.getConfig().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> toFieldType(e.getValue()))),
        createConnector.ifNotExists()
    );
  }

  private static SqlDropConnectorStatement getDropConnectorStatement(final String sql) {
    final Matcher dropConnectorMatcher = DROP_CONNECTOR.matcher(sql);
    if (!dropConnectorMatcher.matches()) {
      throw new MigrationException("Invalid DROP CONNECTOR command: " + sql);
    }
    return new SqlDropConnectorStatement(
        sql,
        dropConnectorMatcher.group(2),
        dropConnectorMatcher.group(1) != null
    );
  }

  private static SqlPropertyCommand getSetPropertyCommand(
      final String sql, final Map<String, String> variables) {
    final Matcher setPropertyMatcher = SET_PROPERTY.matcher(sql);
    if (!setPropertyMatcher.matches()) {
      throw new MigrationException("Invalid SET command: " + sql);
    }
    return new SqlPropertyCommand(
        sql,
        true,
        VariableSubstitutor.substitute(setPropertyMatcher.group(1), variables),
        Optional.of(VariableSubstitutor.substitute(setPropertyMatcher.group(2), variables))
    );
  }

  private static SqlPropertyCommand getUnsetPropertyCommand(
      final String sql, final Map<String, String> variables) {
    final Matcher unsetPropertyMatcher = UNSET_PROPERTY.matcher(sql);
    if (!unsetPropertyMatcher.matches()) {
      throw new MigrationException("Invalid UNSET command: " + sql);
    }
    return new SqlPropertyCommand(
        sql,
        false,
        VariableSubstitutor.substitute(unsetPropertyMatcher.group(1), variables),
        Optional.empty());
  }

  private static SqlDefineVariableCommand getDefineVariableCommand(
      final String sql, final Map<String, String> variables) {
    final Matcher defineVariableMatcher = DEFINE_VARIABLE.matcher(sql);
    if (!defineVariableMatcher.matches()) {
      throw new MigrationException("Invalid DEFINE command: " + sql);
    }
    return new SqlDefineVariableCommand(
        sql,
        defineVariableMatcher.group(1),
        VariableSubstitutor.substitute(defineVariableMatcher.group(2), variables)
    );
  }

  private static SqlUndefineVariableCommand getUndefineVariableCommand(final String sql) {
    final Matcher undefineVariableMatcher = UNDEFINE_VARIABLE.matcher(sql);
    if (!undefineVariableMatcher.matches()) {
      throw new MigrationException("Invalid UNDEFINE command: " + sql);
    }
    return new SqlUndefineVariableCommand(sql, undefineVariableMatcher.group(1));
  }

  private static SqlAssertSchemaCommand getAssertSchemaCommand(
      final String sql, final Map<String, String> variables) {
    final AssertSchema assertSchema;
    try {
      final String substituted = VariableSubstitutor.substitute(
          KSQL_PARSER.parse(sql).get(0),
          variables
      );
      assertSchema = (AssertSchema) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
          "Failed to parse ASSERT SCHEMA statement. Statement: %s. Reason: %s",
          sql, e.getMessage()));
    }
    return new SqlAssertSchemaCommand(
        sql,
        assertSchema.getSubject(),
        assertSchema.getId(),
        assertSchema.checkExists(),
        assertSchema.getTimeout().isPresent()
            ? Optional.of(assertSchema.getTimeout().get().toDuration())
            : Optional.empty()
    );
  }

  private static SqlAssertTopicCommand getAssertTopicCommand(
      final String sql, final Map<String, String> variables) {
    final AssertTopic assertTopic;
    final Map<String, Integer> configs;
    try {
      final String substituted = VariableSubstitutor.substitute(
          KSQL_PARSER.parse(sql).get(0),
          variables
      );
      assertTopic = (AssertTopic) new AstBuilder(TypeRegistry.EMPTY)
          .buildStatement(KSQL_PARSER.parse(substituted).get(0).getStatement());
      configs = assertTopic.getConfig().entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> (Integer) e.getValue().getValue()));
    } catch (ParseFailedException e) {
      throw new MigrationException(String.format(
          "Failed to parse ASSERT TOPIC statement. Statement: %s. Reason: %s",
          sql, e.getMessage()));
    }
    return new SqlAssertTopicCommand(
        sql,
        assertTopic.getTopic(),
        configs,
        assertTopic.checkExists(),
        assertTopic.getTimeout().isPresent()
            ? Optional.of(assertTopic.getTimeout().get().toDuration())
            : Optional.empty()
    );
  }

  /**
   * Determines if a statement is supported and the type of command it is based on a list of tokens.
   */
  private static StatementType getStatementType(final List<String> tokens) {
    if (isInsertValuesStatement(tokens)) {
      return StatementType.INSERT_VALUES;
    } else if (isCreateConnectorStatement(tokens)) {
      return StatementType.CREATE_CONNECTOR;
    } else if (isDropConnectorStatement(tokens)) {
      return StatementType.DROP_CONNECTOR;
    } else if (isSetStatement(tokens)) {
      return StatementType.SET_PROPERTY;
    } else if (isUnsetStatement(tokens)) {
      return StatementType.UNSET_PROPERTY;
    } else if (isDefineStatement(tokens)) {
      return StatementType.DEFINE_VARIABLE;
    } else if (isUndefineStatement(tokens)) {
      return StatementType.UNDEFINE_VARIABLE;
    } else if (isAssertSchema(tokens)) {
      return StatementType.ASSERT_SCHEMA;
    } else if (isAssertTopic(tokens)) {
      return StatementType.ASSERT_TOPIC;
    } else {
      validateSupportedStatementType(tokens);
      return StatementType.STATEMENT;
    }
  }

  private static boolean isAssertTopic(final List<String> tokens) {
    return checkAssert(tokens, TOPIC);
  }

  private static boolean isAssertSchema(final List<String> tokens) {
    return checkAssert(tokens, SCHEMA);
  }

  private static boolean checkAssert(final List<String> tokens, final String assertType) {
    if (tokens.size() >= 1 && tokens.get(0).equals(ASSERT)) {
      final boolean notExists =
          tokens.size() >= 4 && tokens.get(1).equals(NOT) && tokens.get(2).equals(EXISTS);
      if (notExists) {
        return tokens.get(3).equals(assertType);
      } else {
        return tokens.get(1).equals(assertType);
      }
    }
    return false;
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

  private static boolean isSetStatement(final List<String> tokens) {
    return tokens.size() > 0 && tokens.get(0).equals(SET);
  }

  private static boolean isUnsetStatement(final List<String> tokens) {
    return tokens.size() > 0 && tokens.get(0).equals(UNSET);
  }

  private static boolean isDefineStatement(final List<String> tokens) {
    return tokens.size() > 0 && tokens.get(0).equals(DEFINE);
  }

  private static boolean isUndefineStatement(final List<String> tokens) {
    return tokens.size() > 0 && tokens.get(0).equals(UNDEFINE);
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
    private final Map<String, Integer> configs;
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
      this.configs = Objects.requireNonNull(configs);
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
        Optional<String> subject,
        Optional<Integer> id,
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
