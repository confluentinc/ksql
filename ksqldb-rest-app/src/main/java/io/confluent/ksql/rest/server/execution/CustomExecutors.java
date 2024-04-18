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

package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
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
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A suite of {@code StatementExecutor}s that do not need to be distributed.
 * Each handles a corresponding {@code Class<? extends Statement>} and is
 * assumed that the {@code ConfiguredStatement} that is passed in matches the
 * expected class.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public enum CustomExecutors {

  LIST_TOPICS(ListTopics.class, ListTopicsExecutor::execute),
  LIST_STREAMS(ListStreams.class, ListSourceExecutor::streams),
  LIST_TABLES(ListTables.class, ListSourceExecutor::tables),
  DESCRIBE_STREAMS(DescribeStreams.class, ListSourceExecutor::describeStreams),
  DESCRIBE_TABLES(DescribeTables.class, ListSourceExecutor::describeTables),
  LIST_FUNCTIONS(ListFunctions.class, ListFunctionsExecutor::execute),
  LIST_QUERIES(ListQueries.class, ListQueriesExecutor::execute),
  LIST_PROPERTIES(ListProperties.class, ListPropertiesExecutor::execute),
  LIST_CONNECTORS(ListConnectors.class, ListConnectorsExecutor::execute),
  LIST_CONNECTOR_PLUGINS(ListConnectorPlugins.class, ListConnectorPluginsExecutor::execute),
  LIST_TYPES(ListTypes.class, ListTypesExecutor::execute),
  LIST_VARIABLES(ListVariables.class, ListVariablesExecutor::execute),
  ASSERT_TOPIC(AssertTopic.class, AssertTopicExecutor::execute),
  ASSERT_SCHEMA(AssertSchema.class, AssertSchemaExecutor::execute),

  SHOW_COLUMNS(ShowColumns.class, ListSourceExecutor::columns),
  EXPLAIN(Explain.class, ExplainExecutor::execute),
  DESCRIBE_FUNCTION(DescribeFunction.class, DescribeFunctionExecutor::execute),
  SET_PROPERTY(SetProperty.class, PropertyExecutor::set),
  UNSET_PROPERTY(UnsetProperty.class, PropertyExecutor::unset),
  DEFINE_VARIABLE(DefineVariable.class, VariableExecutor::set),
  UNDEFINE_VARIABLE(UndefineVariable.class, VariableExecutor::unset),
  INSERT_VALUES(InsertValues.class, insertValuesExecutor()),
  CREATE_CONNECTOR(CreateConnector.class, ConnectExecutor::execute),
  DROP_CONNECTOR(DropConnector.class, DropConnectorExecutor::execute),
  DESCRIBE_CONNECTOR(DescribeConnector.class, new DescribeConnectorExecutor()::execute),
  TERMINATE_QUERY(TerminateQuery.class, TerminateQueryExecutor::execute)
  ;

  public static final Map<Class<? extends Statement>, StatementExecutor<?>> EXECUTOR_MAP =
      ImmutableMap.copyOf(
          EnumSet.allOf(CustomExecutors.class)
              .stream()
              .collect(Collectors.toMap(
                  CustomExecutors::getStatementClass,
                  CustomExecutors::getExecutor))
      );

  private final Class<? extends Statement> statementClass;
  private final StatementExecutor executor;

  <T extends Statement> CustomExecutors(
      final Class<T> statementClass,
      final StatementExecutor<? super T> executor
  ) {
    this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  private Class<? extends Statement> getStatementClass() {
    return statementClass;
  }

  private StatementExecutor<?> getExecutor() {
    return this::execute;
  }

  public StatementExecutorResponse execute(
      final ConfiguredStatement<?> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionCtx,
      final ServiceContext serviceCtx
  ) {
    return executor.execute(
        statement,
        sessionProperties,
        executionCtx,
        serviceCtx
    );
  }

  private static StatementExecutor insertValuesExecutor() {
    final InsertValuesExecutor executor = new InsertValuesExecutor();

    return (
        statement,
        sessionProperties,
        executionContext,
        serviceContext
    ) -> {
      executor.execute(
          statement,
          sessionProperties,
          executionContext,
          serviceContext
      );
      return StatementExecutorResponse.handled(Optional.empty());
    };
  }
}
