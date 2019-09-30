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

package io.confluent.ksql.engine;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Executor of {@code PreparedStatement} within a specific {@code EngineContext} and using a
 * specific set of config.
 * </p>
 * All statements are executed using a {@code ServiceContext} specified in the constructor. This
 * {@code ServiceContext} might have been initialized with limited permissions to access Kafka
 * resources. The {@code EngineContext} has an internal {@code ServiceContext} that might have more
 * or less permissions than the one specified. This approach is useful when KSQL needs to
 * impersonate the current REST user executing the statements.
 */
final class EngineExecutor {

  private final EngineContext engineContext;
  private final ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig;
  private final Map<String, Object> overriddenProperties;

  private EngineExecutor(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.overriddenProperties =
        Objects.requireNonNull(overriddenProperties, "overriddenProperties");

    KsqlEngineProps.throwOnImmutableOverride(overriddenProperties);
  }

  static EngineExecutor create(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return new EngineExecutor(engineContext, serviceContext, ksqlConfig, overriddenProperties);
  }

  ExecuteResult execute(final ConfiguredStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);

      if (statement.getStatement() instanceof Query) {
        return executeQuery(statement, (Query)statement.getStatement(), Optional.empty());
      }
      if (statement.getStatement() instanceof QueryContainer) {
        return executeQuery(
            statement,
            ((QueryContainer)statement.getStatement()).getQuery(),
            Optional.of(((QueryContainer)statement.getStatement()).getSink())
        );
      }
      return executeDdl(statement);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private ExecuteResult executeQuery(final ConfiguredStatement<?> statement,
      final Query query,
      final Optional<Sink> sink) {

    final QueryEngine queryEngine = engineContext.createQueryEngine(serviceContext);

    final OutputNode outputNode = QueryEngine.buildQueryLogicalPlan(
        query,
        sink,
        engineContext.getMetaStore(),
        ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties)
    );

    final LogicalPlanNode logicalPlan =
        new LogicalPlanNode(statement.getStatementText(), Optional.of(outputNode));

    final QueryMetadata queryMetadata = queryEngine.buildPhysicalPlan(
        logicalPlan,
        ksqlConfig,
        overriddenProperties,
        engineContext.getMetaStore()
    );

    validateQuery(queryMetadata, statement);

    engineContext.registerQuery(queryMetadata);

    return ExecuteResult.of(queryMetadata);
  }

  private ExecuteResult executeDdl(final ConfiguredStatement<?> statement) {
    final String msg = engineContext.executeDdlStatement(
        statement.getStatementText(),
        (ExecutableDdlStatement) statement.getStatement(),
        ksqlConfig,
        overriddenProperties
    );

    return ExecuteResult.of(msg);
  }

  private void validateQuery(
      final QueryMetadata query,
      final ConfiguredStatement<?> statement
  ) {
    if (statement.getStatement() instanceof CreateStreamAsSelect
        && query.getDataSourceType() == DataSourceType.KTABLE) {
      throw new KsqlStatementException("Invalid result type. "
          + "Your SELECT query produces a TABLE. "
          + "Please use CREATE TABLE AS SELECT statement instead.",
          statement.getStatementText());
    }

    if (statement.getStatement() instanceof CreateTableAsSelect
        && query.getDataSourceType() == DataSourceType.KSTREAM) {
      throw new KsqlStatementException("Invalid result type. "
          + "Your SELECT query produces a STREAM. "
          + "Please use CREATE STREAM AS SELECT statement instead.",
          statement.getStatementText());
    }

    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();

      AvroUtil.throwOnInvalidSchemaEvolution(persistentQuery, srClient);
    }
  }

  private static void throwOnNonExecutableStatement(final ConfiguredStatement<?> statement) {
    if (!KsqlEngine.isExecutableStatement(statement.getStatement())) {
      throw new KsqlStatementException("Statement not executable", statement.getStatementText());
    }
  }
}
