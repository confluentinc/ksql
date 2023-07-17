/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.validation;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.parser.tree.AssertTopic;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.DefineVariable;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.ListConnectorPlugins;
import io.confluent.ksql.parser.tree.ListConnectors;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.ListTypes;
import io.confluent.ksql.parser.tree.ListVariables;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.execution.ConnectExecutor;
import io.confluent.ksql.rest.server.execution.DescribeFunctionExecutor;
import io.confluent.ksql.rest.server.execution.ExplainExecutor;
import io.confluent.ksql.rest.server.execution.InsertValuesExecutor;
import io.confluent.ksql.rest.server.execution.ListSourceExecutor;
import io.confluent.ksql.rest.server.execution.ListVariablesExecutor;
import io.confluent.ksql.rest.server.execution.PropertyExecutor;
import io.confluent.ksql.rest.server.execution.VariableExecutor;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A set of {@code StatementValidator}s which are used to validate non-executable
 * statements. Each handles a corresponding {@code Class<? extends Statement>} and
 * is assumed that the {@code ConfiguredStatement} that is passed in matches the
 * expected class.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public enum CustomValidators {
  QUERY_ENDPOINT(Query.class,
      (statement,
          sessionProperties,
          executionContext,
          serviceContext) -> {
        throw new KsqlRestException(Errors.queryEndpoint(statement.getMaskedStatementText()));
      }),
  PRINT_TOPIC(PrintTopic.class, PrintTopicValidator::validate),
  ALTER_SYSTEM_PROPERTY(AlterSystemProperty.class, StatementValidator.NO_VALIDATION),
  LIST_TOPICS(ListTopics.class, StatementValidator.NO_VALIDATION),
  LIST_STREAMS(ListStreams.class, StatementValidator.NO_VALIDATION),
  LIST_TABLES(ListTables.class, StatementValidator.NO_VALIDATION),
  DESCRIBE_STREAMS(DescribeStreams.class, StatementValidator.NO_VALIDATION),
  DESCRIBE_TABLES(DescribeTables.class, StatementValidator.NO_VALIDATION),
  LIST_FUNCTIONS(ListFunctions.class, StatementValidator.NO_VALIDATION),
  LIST_QUERIES(ListQueries.class, StatementValidator.NO_VALIDATION),
  LIST_PROPERTIES(ListProperties.class, StatementValidator.NO_VALIDATION),
  LIST_CONNECTORS(ListConnectors.class, StatementValidator.NO_VALIDATION),
  LIST_CONNECTOR_PLUGINS(ListConnectorPlugins.class, StatementValidator.NO_VALIDATION),
  LIST_TYPES(ListTypes.class, StatementValidator.NO_VALIDATION),
  CREATE_CONNECTOR(CreateConnector.class, ConnectExecutor::validate),
  DROP_CONNECTOR(DropConnector.class, StatementValidator.NO_VALIDATION),
  ASSERT_TOPIC(AssertTopic.class, StatementValidator.NO_VALIDATION),
  ASSERT_SCHEMA(AssertSchema.class, StatementValidator.NO_VALIDATION),
  LIST_VARIABLES(ListVariables.class, ListVariablesExecutor::execute),

  INSERT_VALUES(InsertValues.class, new InsertValuesExecutor()::execute),
  SHOW_COLUMNS(ShowColumns.class, ListSourceExecutor::columns),
  EXPLAIN(Explain.class, ExplainExecutor::execute),
  DESCRIBE_FUNCTION(DescribeFunction.class, DescribeFunctionExecutor::execute),
  DESCRIBE_CONNECTOR(DescribeConnector.class, StatementValidator.NO_VALIDATION),
  SET_PROPERTY(SetProperty.class, PropertyExecutor::set),
  UNSET_PROPERTY(UnsetProperty.class, PropertyExecutor::unset),
  DEFINE_VARIABLE(DefineVariable.class, VariableExecutor::set),
  UNDEFINE_VARIABLE(UndefineVariable.class, VariableExecutor::unset);

  public static final Map<Class<? extends Statement>, StatementValidator<?>> VALIDATOR_MAP =
      ImmutableMap.copyOf(
        EnumSet.allOf(CustomValidators.class)
            .stream()
            .collect(Collectors.toMap(
                CustomValidators::getStatementClass,
                CustomValidators::getValidator))
      );

  private final Class<? extends Statement> statementClass;
  private final StatementValidator validator;

  <T extends Statement> CustomValidators(
      final Class<T> statementClass,
      final StatementValidator<? super T> validator
  ) {
    this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  private Class<? extends Statement> getStatementClass() {
    return statementClass;
  }

  private StatementValidator<?> getValidator() {
    return this::validate;
  }

  public void validate(
      final ConfiguredStatement<?> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) throws KsqlException {
    validator.validate(
        statement,
        sessionProperties,
        executionContext,
        serviceContext);
  }
}
