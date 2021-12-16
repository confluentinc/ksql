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

import com.google.common.annotations.VisibleForTesting;
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
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A suite of {@link StatementExecutor}s that do not need to be distributed. Each handles a
 * corresponding {@code Class<? extends Statement>} and is assumed that the {@link
 * ConfiguredStatement} that is passed in matches the expected class.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CustomExecutors {

  @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
  public final Map<Class<? extends Statement>, StatementExecutor<?>> EXECUTOR_MAP;

  public CustomExecutors(final ConnectServerErrors connectErrorHandler) {
    Objects.requireNonNull(connectErrorHandler, "connectErrorHandler");

    EXECUTOR_MAP = new HashMap<Class<? extends Statement>, StatementExecutor<?>>() {{
        put(ListTopics.class,
            (StatementExecutor<ListTopics>) ListTopicsExecutor::execute);
        put(ListStreams.class,
            (StatementExecutor<ListStreams>) ListSourceExecutor::streams);
        put(ListTables.class,
            (StatementExecutor<ListTables>) ListSourceExecutor::tables);
        put(DescribeStreams.class,
            (StatementExecutor<DescribeStreams>) ListSourceExecutor::describeStreams);
        put(DescribeTables.class,
            (StatementExecutor<DescribeTables>) ListSourceExecutor::describeTables);
        put(ListFunctions.class,
            (StatementExecutor<ListFunctions>) ListFunctionsExecutor::execute);
        put(ListQueries.class,
            (StatementExecutor<ListQueries>) ListQueriesExecutor::execute);
        put(ListProperties.class,
            (StatementExecutor<ListProperties>) ListPropertiesExecutor::execute);
        put(ListConnectors.class,
            (StatementExecutor<ListConnectors>)
                new ListConnectorsExecutor(connectErrorHandler)::execute);
        put(ListConnectorPlugins.class,
            (StatementExecutor<ListConnectorPlugins>)
                new ListConnectorPluginsExecutor(connectErrorHandler)::execute);
        put(ListTypes.class,
            (StatementExecutor<ListTypes>) ListTypesExecutor::execute);
        put(ListVariables.class,
            (StatementExecutor<ListVariables>) ListVariablesExecutor::execute);
        put(ShowColumns.class,
            (StatementExecutor<ShowColumns>) ListSourceExecutor::columns);
        put(Explain.class,
            (StatementExecutor<Explain>) ExplainExecutor::execute);
        put(DescribeFunction.class,
            (StatementExecutor<DescribeFunction>) DescribeFunctionExecutor::execute);
        put(SetProperty.class,
            (StatementExecutor<SetProperty>) PropertyExecutor::set);
        put(UnsetProperty.class, (StatementExecutor<UnsetProperty>) PropertyExecutor::unset);
        put(DefineVariable.class, (StatementExecutor<DefineVariable>) VariableExecutor::set);
        put(UndefineVariable.class, (StatementExecutor<UndefineVariable>) VariableExecutor::unset);
        put(InsertValues.class, insertValuesExecutor());
        put(CreateConnector.class,
            (StatementExecutor<CreateConnector>) new ConnectExecutor(connectErrorHandler)::execute);
        put(DropConnector.class,
            (StatementExecutor<DropConnector>)
                new DropConnectorExecutor(connectErrorHandler)::execute);
        put(DescribeConnector.class,
            (StatementExecutor<DescribeConnector>)
                new DescribeConnectorExecutor(connectErrorHandler)::execute);
        put(TerminateQuery.class,
            (StatementExecutor<TerminateQuery>) TerminateQueryExecutor::execute);
      }};
  }

  @VisibleForTesting
  StatementExecutor<Explain> explain() {
    return (StatementExecutor<Explain>) EXECUTOR_MAP.get(Explain.class);
  }

  @VisibleForTesting
  StatementExecutor<ListTopics> listTopics() {
    return (StatementExecutor<ListTopics>) EXECUTOR_MAP.get(ListTopics.class);
  }

  @VisibleForTesting
  StatementExecutor<TerminateQuery> terminateQuery() {
    return (StatementExecutor<TerminateQuery>) EXECUTOR_MAP.get(TerminateQuery.class);
  }

  @VisibleForTesting
  StatementExecutor<DefineVariable> defineVariable() {
    return (StatementExecutor<DefineVariable>) EXECUTOR_MAP.get(DefineVariable.class);
  }

  @VisibleForTesting
  StatementExecutor<UndefineVariable> undefineVariable() {
    return (StatementExecutor<UndefineVariable>) EXECUTOR_MAP.get(UndefineVariable.class);
  }

  @VisibleForTesting
  StatementExecutor<SetProperty> setProperty() {
    return (StatementExecutor<SetProperty>) EXECUTOR_MAP.get(SetProperty.class);
  }

  @VisibleForTesting
  StatementExecutor<UnsetProperty> unsetProperty() {
    return (StatementExecutor<UnsetProperty>) EXECUTOR_MAP.get(UnsetProperty.class);
  }

  @VisibleForTesting
  StatementExecutor<ListProperties> listProperties() {
    return (StatementExecutor<ListProperties>) EXECUTOR_MAP.get(ListProperties.class);
  }

  @VisibleForTesting
  StatementExecutor<ListStreams> listStreams() {
    return (StatementExecutor<ListStreams>) EXECUTOR_MAP.get(ListStreams.class);
  }

  @VisibleForTesting
  StatementExecutor<DescribeStreams> describeStreams() {
    return (StatementExecutor<DescribeStreams>) EXECUTOR_MAP.get(DescribeStreams.class);
  }

  @VisibleForTesting
  StatementExecutor<ListTables> listTables() {
    return (StatementExecutor<ListTables>) EXECUTOR_MAP.get(ListTables.class);
  }

  @VisibleForTesting
  StatementExecutor<DescribeTables> describeTables() {
    return (StatementExecutor<DescribeTables>) EXECUTOR_MAP.get(DescribeTables.class);
  }

  @VisibleForTesting
  StatementExecutor<ShowColumns> showColumns() {
    return (StatementExecutor<ShowColumns>) EXECUTOR_MAP.get(ShowColumns.class);
  }

  @VisibleForTesting
  StatementExecutor<ListFunctions> listFunctions() {
    return (StatementExecutor<ListFunctions>) EXECUTOR_MAP.get(ListFunctions.class);
  }

  @VisibleForTesting
  StatementExecutor<DescribeFunction> describeFunction() {
    return (StatementExecutor<DescribeFunction>) EXECUTOR_MAP.get(DescribeFunction.class);
  }

  @VisibleForTesting
  StatementExecutor<ListQueries> listQueries() {
    return (StatementExecutor<ListQueries>) EXECUTOR_MAP.get(ListQueries.class);
  }

  @VisibleForTesting
  StatementExecutor<ListVariables> listVariables() {
    return (StatementExecutor<ListVariables>) EXECUTOR_MAP.get(ListVariables.class);
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
