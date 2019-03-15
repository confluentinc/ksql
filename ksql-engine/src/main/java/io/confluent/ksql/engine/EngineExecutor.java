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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Objects;

/**
 * Executor of {@code PreparedStatement} within a specific {@code EngineContext} and using a
 * specific set of config.
 */
final class EngineExecutor {

  private final EngineContext engineContext;
  private final KsqlConfig ksqlConfig;
  private final Map<String, Object> overriddenProperties;

  private EngineExecutor(
      final EngineContext engineContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.overriddenProperties =
        Objects.requireNonNull(overriddenProperties, "overriddenProperties");

    KsqlEngineProps.throwOnImmutableOverride(overriddenProperties);
  }

  static EngineExecutor create(
      final EngineContext engineContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return new EngineExecutor(engineContext, ksqlConfig, overriddenProperties);
  }

  ExecuteResult execute(final PreparedStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);

      final QueryEngine queryEngine = engineContext.createQueryEngine();

      final LogicalPlanNode logicalPlan = queryEngine.buildLogicalPlan(
          engineContext.getMetaStore(),
          statement,
          ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties)
      );

      if (logicalPlan.getNode() == null) {
        final String msg = engineContext.executeDdlStatement(
            statement.getStatementText(),
            (ExecutableDdlStatement) statement.getStatement(),
            overriddenProperties
        );

        return ExecuteResult.of(msg);
      }

      final QueryMetadata query = queryEngine.buildPhysicalPlan(
          logicalPlan,
          ksqlConfig,
          overriddenProperties,
          engineContext.getServiceContext().getKafkaClientSupplier(),
          engineContext.getMetaStore()
      );

      validateQuery(query, statement);

      engineContext.registerQuery(query);

      return ExecuteResult.of(query);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private void validateQuery(final QueryMetadata query, final PreparedStatement<?> statement) {
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
      final SchemaRegistryClient srClient = engineContext.getServiceContext()
          .getSchemaRegistryClient();

      if (!AvroUtil.isValidSchemaEvolution(persistentQuery, srClient)) {
        throw new KsqlStatementException(String.format(
            "Cannot register avro schema for %s as the schema registry rejected it, "
                + "(maybe schema evolution issues?)",
            persistentQuery.getResultTopic().getKafkaTopicName()),
            statement.getStatementText());
      }
    }
  }

  private static void throwOnNonExecutableStatement(final PreparedStatement<?> statement) {
    if (!KsqlEngine.isExecutableStatement(statement)) {
      throw new KsqlStatementException("Statement not executable", statement.getStatementText());
    }
  }
}
