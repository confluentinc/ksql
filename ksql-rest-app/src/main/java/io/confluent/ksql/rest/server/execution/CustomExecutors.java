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
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A suite of {@code StatementExecutor}s that do not need to be distributed.
 * Each handles a corresponding {@code Class<? extends Statement>} and is
 * assumed that the {@code PreparedStatement} that is passed in matches the
 * expected class.
 */
public enum CustomExecutors implements StatementExecutor {

  LIST_TOPICS(ListTopics.class, ListTopicsExecutor::execute),
  LIST_REGISTERED_TOPICS(ListRegisteredTopics.class, ListRegisteredTopicsExecutor::execute),
  LIST_STREAMS(ListStreams.class, ListSourceExecutor::streams),
  LIST_TABLES(ListTables.class, ListSourceExecutor::tables),
  LIST_FUNCTIONS(ListFunctions.class, ListFunctionsExecutor::execute),
  LIST_QUERIES(ListQueries.class, ListQueriesExecutor::execute),
  LIST_PROPERTIES(ListProperties.class, ListPropertiesExecutor::execute),

  SHOW_COLUMNS(ShowColumns.class, ListSourceExecutor::columns),
  EXPLAIN(Explain.class, ExplainExecutor::execute),
  DESCRIBE_FUNCTION(DescribeFunction.class, DescribeFunctionExecutor::execute),
  SET_PROPERTY(SetProperty.class, PropertyExecutor::set),
  UNSET_PROPERTY(UnsetProperty.class, PropertyExecutor::unset);

  public static final Map<Class<? extends Statement>, StatementExecutor> EXECUTOR_MAP =
      ImmutableMap.copyOf(
          EnumSet.allOf(CustomExecutors.class)
              .stream()
              .collect(Collectors.toMap(
                  CustomExecutors::getStatementClass,
                  Function.identity()))
      );

  private final Class<? extends Statement> statementClass;
  private final StatementExecutor executor;

  CustomExecutors(
      final Class<? extends Statement> statementClass,
      final StatementExecutor executor) {
    this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  private Class<? extends Statement> getStatementClass() {
    return statementClass;
  }

  @Override
  public Optional<KsqlEntity> execute(
      final PreparedStatement<?> statement,
      final KsqlExecutionContext executionCtx,
      final ServiceContext serviceCtx,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    return executor.execute(statement, executionCtx, serviceCtx, ksqlConfig, propertyOverrides);
  }
}
