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
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryExecutor;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.AvroUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PlanSummary;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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

  ExecuteResult execute(final KsqlPlan plan) {
    if (!plan.getQueryPlan().isPresent()) {
      final String ddlResult = plan
          .getDdlCommand()
          .map(ddl -> executeDdl(ddl, plan.getStatementText(), false))
          .orElseThrow(
              () -> new IllegalStateException(
                  "DdlResult should be present if there is no physical plan."));
      return ExecuteResult.of(ddlResult);
    }

    final QueryPlan queryPlan = plan.getQueryPlan().get();
    plan.getDdlCommand().map(ddl -> executeDdl(ddl, plan.getStatementText(), true));
    return ExecuteResult.of(executePersistentQuery(queryPlan, plan.getStatementText()));
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent") // Known to be non-empty
  TransientQueryMetadata executeQuery(final ConfiguredStatement<Query> statement) {
    final ExecutorPlans plans = planQuery(statement, statement.getStatement(), Optional.empty());
    final OutputNode outputNode = plans.logicalPlan.getNode().get();
    final QueryExecutor executor = engineContext.createQueryExecutor(
        ksqlConfig,
        overriddenProperties,
        serviceContext
    );
    return executor.buildTransientQuery(
        statement.getStatementText(),
        plans.physicalPlan.getQueryId(),
        getSourceNames(outputNode),
        plans.physicalPlan.getPhysicalPlan(),
        buildPlanSummary(
            plans.physicalPlan.getQueryId(),
            plans.physicalPlan.getPhysicalPlan()),
        outputNode.getSchema(),
        outputNode.getLimit()
    );
  }

  // Known to be non-empty
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  KsqlPlan plan(final ConfiguredStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);

      if (statement.getStatement() instanceof ExecutableDdlStatement) {
        final DdlCommand ddlCommand = engineContext.createDdlCommand(
            statement.getStatementText(),
            (ExecutableDdlStatement) statement.getStatement(),
            ksqlConfig,
            overriddenProperties
        );

        return KsqlPlan.ddlPlanCurrent(statement.getStatementText(), ddlCommand);
      }

      final QueryContainer queryContainer = (QueryContainer) statement.getStatement();
      final ExecutorPlans plans = planQuery(
          statement,
          queryContainer.getQuery(),
          Optional.of(queryContainer.getSink())
      );

      final KsqlStructuredDataOutputNode outputNode =
          (KsqlStructuredDataOutputNode) plans.logicalPlan.getNode().get();

      final Optional<DdlCommand> ddlCommand = maybeCreateSinkDdl(
          statement,
          outputNode
      );

      validateQuery(outputNode.getNodeOutputType(), statement);

      final QueryPlan queryPlan = new QueryPlan(
          getSourceNames(outputNode),
          outputNode.getIntoSourceName(),
          plans.physicalPlan.getPhysicalPlan(),
          plans.physicalPlan.getQueryId()
      );

      return KsqlPlan.queryPlanCurrent(
          statement.getStatementText(),
          ddlCommand,
          queryPlan
      );
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private ExecutorPlans planQuery(
      final ConfiguredStatement<?> statement,
      final Query query,
      final Optional<Sink> sink) {
    final QueryEngine queryEngine = engineContext.createQueryEngine(serviceContext);
    final KsqlConfig config = this.ksqlConfig.cloneWithPropertyOverwrite(overriddenProperties);
    final OutputNode outputNode = QueryEngine.buildQueryLogicalPlan(
        query,
        sink,
        engineContext.getMetaStore(),
        config
    );
    final LogicalPlanNode logicalPlan = new LogicalPlanNode(
        statement.getStatementText(),
        Optional.of(outputNode)
    );
    final QueryId queryId = QueryIdUtil.buildId(
        engineContext.getMetaStore(),
        engineContext.idGenerator(),
        outputNode,
        config.getBoolean(KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED)
    );
    final PhysicalPlan physicalPlan = queryEngine.buildPhysicalPlan(
        logicalPlan,
        ksqlConfig,
        overriddenProperties,
        engineContext.getMetaStore(),
        queryId
    );
    return new ExecutorPlans(logicalPlan, physicalPlan);
  }

  private static final class ExecutorPlans {
    private final LogicalPlanNode logicalPlan;
    private final PhysicalPlan physicalPlan;

    private ExecutorPlans(
        final LogicalPlanNode logicalPlan,
        final PhysicalPlan physicalPlan) {
      this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
      this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlanNode");
    }
  }

  private Optional<DdlCommand> maybeCreateSinkDdl(
      final ConfiguredStatement<?> cfgStatement,
      final KsqlStructuredDataOutputNode outputNode
  ) {
    if (!outputNode.createInto()) {
      validateExistingSink(outputNode);
      return Optional.empty();
    }

    final Formats formats = Formats.of(
        outputNode.getKsqlTopic().getKeyFormat(),
        outputNode.getKsqlTopic().getValueFormat(),
        outputNode.getSerdeOptions()
    );

    final Statement statement = cfgStatement.getStatement();
    final CreateSourceCommand ddl;
    if (outputNode.getNodeOutputType() == DataSourceType.KSTREAM) {
      ddl = new CreateStreamCommand(
          outputNode.getIntoSourceName(),
          outputNode.getSchema(),
          outputNode.getTimestampColumn(),
          outputNode.getKsqlTopic().getKafkaTopicName(),
          formats,
          outputNode.getKsqlTopic().getKeyFormat().getWindowInfo(),
          Optional.of(
              statement instanceof CreateAsSelect && ((CreateAsSelect) statement).isOrReplace())
      );
    } else {
      ddl = new CreateTableCommand(
          outputNode.getIntoSourceName(),
          outputNode.getSchema(),
          outputNode.getTimestampColumn(),
          outputNode.getKsqlTopic().getKafkaTopicName(),
          formats,
          outputNode.getKsqlTopic().getKeyFormat().getWindowInfo(),
          Optional.of(
              statement instanceof CreateAsSelect && ((CreateAsSelect) statement).isOrReplace())
      );
    }

    final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();
    AvroUtil.throwOnInvalidSchemaEvolution(cfgStatement.getStatementText(), ddl, srClient);
    return Optional.of(ddl);
  }

  private void validateExistingSink(
      final KsqlStructuredDataOutputNode outputNode
  ) {
    final SourceName name = outputNode.getIntoSourceName();
    final DataSource existing = engineContext.getMetaStore().getSource(name);

    if (existing == null) {
      throw new KsqlException(String.format("%s does not exist.", outputNode));
    }

    if (existing.getDataSourceType() != outputNode.getNodeOutputType()) {
      throw new KsqlException(String.format("Incompatible data sink and query result. Data sink"
              + " (%s) type is %s but select query result is %s.",
          name.text(),
          existing.getDataSourceType(),
          outputNode.getNodeOutputType())
      );
    }

    final LogicalSchema resultSchema = outputNode.getSchema();
    final LogicalSchema existingSchema = existing.getSchema();

    if (!resultSchema.compatibleSchema(existingSchema)) {
      throw new KsqlException("Incompatible schema between results and sink."
          + System.lineSeparator()
          + "Result schema is " + resultSchema
          + System.lineSeparator()
          + "Sink schema is " + existingSchema
      );
    }
  }

  private static void validateQuery(
      final DataSourceType dataSourceType,
      final ConfiguredStatement<?> statement
  ) {
    if (statement.getStatement() instanceof CreateStreamAsSelect
        && dataSourceType == DataSourceType.KTABLE) {
      throw new KsqlStatementException("Invalid result type. "
          + "Your SELECT query produces a TABLE. "
          + "Please use CREATE TABLE AS SELECT statement instead.",
          statement.getStatementText());
    }

    if (statement.getStatement() instanceof CreateTableAsSelect
        && dataSourceType == DataSourceType.KSTREAM) {
      throw new KsqlStatementException("Invalid result type. "
          + "Your SELECT query produces a STREAM. "
          + "Please use CREATE STREAM AS SELECT statement instead.",
          statement.getStatementText());
    }
  }

  private static void throwOnNonExecutableStatement(final ConfiguredStatement<?> statement) {
    if (!KsqlEngine.isExecutableStatement(statement.getStatement())) {
      throw new KsqlStatementException("Statement not executable", statement.getStatementText());
    }
  }

  private static Set<SourceName> getSourceNames(final PlanNode outputNode) {
    return outputNode.getSourceNodes()
        .map(DataSourceNode::getDataSource)
        .map(DataSource::getName)
        .collect(Collectors.toSet());
  }

  private String executeDdl(
      final DdlCommand ddlCommand,
      final String statementText,
      final boolean withQuery
  ) {
    try {
      return engineContext.executeDdl(statementText, ddlCommand, withQuery);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlStatementException(e.getMessage(), statementText, e);
    }
  }

  private PersistentQueryMetadata executePersistentQuery(
      final QueryPlan queryPlan,
      final String statementText
  ) {
    final QueryExecutor executor = engineContext.createQueryExecutor(
        ksqlConfig,
        overriddenProperties,
        serviceContext
    );

    final PersistentQueryMetadata queryMetadata = executor.buildPersistentQuery(
        statementText,
        queryPlan.getQueryId(),
        engineContext.getMetaStore().getSource(queryPlan.getSink()),
        queryPlan.getSources(),
        queryPlan.getPhysicalPlan(),
        buildPlanSummary(queryPlan.getQueryId(), queryPlan.getPhysicalPlan())
    );

    engineContext.registerQuery(queryMetadata);
    return queryMetadata;
  }

  private String buildPlanSummary(final QueryId queryId, final ExecutionStep<?> plan) {
    return new PlanSummary(queryId, ksqlConfig, engineContext.getMetaStore()).summarize(plan);
  }
}
