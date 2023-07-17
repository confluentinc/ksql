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

package io.confluent.ksql.rest.server.computation;

import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates commands that have been validated to successfully execute against
 * the given engine snapshot. Validated commands are safe to enqueue onto the
 * command queue.
 */
public final class ValidatedCommandFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatedCommandFactory.class);

  /**
   * Create a validated command.
   * @param statement The KSQL statement to create the command for.
   * @param context The KSQL engine snapshot to validate the command against.
   * @return A validated command, which is safe to enqueue onto the command topic.
   */
  public Command create(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context) {
    return create(statement, context.getServiceContext(), context);
  }

  /**
   * Create a validated command using the supplied service context
   * @param statement The KSQL statement to create the command for.
   * @param serviceContext The KSQL service context.
   * @param context The KSQL engine snapshot to validate the command against.
   * @return A validated command, which is safe to enqueue onto the command topic.
   */
  @SuppressWarnings("MethodMayBeStatic") // Not static to allow dependency injection
  public Command create(
      final ConfiguredStatement<? extends Statement> statement,
      final ServiceContext serviceContext,
      final KsqlExecutionContext context
  ) {
    return ensureDeserializable(createCommand(statement, serviceContext, context));
  }

  /**
   * Ensure any command written to the command topic can be deserialized.
   *
   * <p>Any command that can't be deserialized is a bug. However, given a non-deserializable
   * command will kill the command runner thread, this is a safety net to ensure commands written to
   * the command topic can be deserialzied.
   *
   * @param command the command to test.
   * @return the passed in command.
   */
  private static Command ensureDeserializable(final Command command) {
    try {
      final String json = PlanJsonMapper.INSTANCE.get().writeValueAsString(command);
      PlanJsonMapper.INSTANCE.get().readValue(json, Command.class);
      return command;
    } catch (final JsonProcessingException e) {
      throw new KsqlServerException("Did not write the command to the command topic "
          + "as it could not be deserialized. This is a bug! Please raise a Github issue "
          + "containing the series of commands you ran to get to this point."
          + System.lineSeparator()
          + e.getMessage());
    }
  }

  private static Command createCommand(
      final ConfiguredStatement<? extends Statement> statement,
      final ServiceContext serviceContext,
      final KsqlExecutionContext context
  ) {
    if (statement.getUnMaskedStatementText().equals(
        TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT)) {
      return createForTerminateCluster(statement);
    }

    if (statement.getStatement() instanceof PauseQuery) {
      return createForPauseQuery(statement, context);
    }

    if (statement.getStatement() instanceof ResumeQuery) {
      return createForResumeQuery(statement, context);
    }

    if (statement.getStatement() instanceof TerminateQuery) {
      return createForTerminateQuery(statement, context);
    }

    if (statement.getStatement() instanceof AlterSystemProperty) {
      return createForAlterSystemQuery(statement, context);
    }

    return createForPlannedQuery(statement.withConfig(context.getKsqlConfig()),
        serviceContext, context);
  }

  private static Command createForAlterSystemQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final AlterSystemProperty alterSystemProperty = (AlterSystemProperty) statement.getStatement();
    final String propertyName = alterSystemProperty.getPropertyName();
    final String propertyValue = alterSystemProperty.getPropertyValue();

    // raise exception if feature flag is set
    if (!context.getKsqlConfig().getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)) {
      throw new KsqlServerException("Cannot alter system configs "
          + "when KSQL_SHARED_RUNTIME_ENABLED is turned off.");
    }

    // validate
    context.alterSystemProperty(propertyName, propertyValue);
    if (!Property.isEditable(propertyName)) {
      throw new ConfigException(
          String.format("Failed to set %s to %s. Caused by: "
                  + "Not recognizable as ksql, streams, consumer, or producer property: %s %n",
              propertyName, propertyValue, propertyName), null);
    }

    // verify that no persistent query is running when attempting to change 'processing.guarantee'
    final KsqlConfigResolver resolver = new KsqlConfigResolver();
    final Optional<ConfigItem> resolvedItem = resolver.resolve(propertyName, false);
    if (resolvedItem.isPresent()
        && Objects.equals(resolvedItem.get().getPropertyName(), PROCESSING_GUARANTEE_CONFIG)
        && !context.getPersistentQueries().isEmpty()) {
      final Collection<QueryId> runningQueries =
          context.getPersistentQueries()
              .stream()
              .map(QueryMetadata::getQueryId)
              .collect(Collectors.toList());
      LOG.error("Failed to set {} to {} due to the {} persistent queries currently running: {}",
                propertyName, propertyValue, runningQueries.size(), runningQueries);
      throw new ConfigException(
          String.format("Unable to set %s to %s, as the %s may not be changed for running"
                            + " persistent queries which have already processed data under a"
                            + " different %s. To modify %s you must first terminate all running"
                            + " persistent queries.",
                        propertyName, propertyValue, propertyName, propertyName, propertyName));
    }

    return Command.of(statement);
  }

  private static Command createForPauseQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final PauseQuery pauseQuery = (PauseQuery) statement.getStatement();
    final Optional<QueryId> queryId = pauseQuery.getQueryId();

    if (!queryId.isPresent()) {
      context.getPersistentQueries().forEach(PersistentQueryMetadata::pause);
      return Command.of(statement);
    } else if (queryId.get().toString().toLowerCase()
        .contains(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT)) {
      return Command.of(statement);
    }

    final PersistentQueryMetadata queryMetadata = context.getPersistentQuery(queryId.get())
        .orElseThrow(() -> new KsqlStatementException(
            "Unknown queryId: " + queryId.get(),
            statement.getMaskedStatementText()));

    if (queryMetadata.getPersistentQueryType() == KsqlConstants.PersistentQueryType.CREATE_SOURCE) {
      throw new KsqlStatementException(
          String.format("Cannot pause query '%s' because it is linked to a source table.",
              queryId.get()), statement.getMaskedStatementText());
    }

    queryMetadata.pause();
    return Command.of(statement);
  }

  private static Command createForResumeQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final ResumeQuery resumeQuery = (ResumeQuery) statement.getStatement();
    final Optional<QueryId> queryId = resumeQuery.getQueryId();

    if (!queryId.isPresent()) {
      context.getPersistentQueries().forEach(PersistentQueryMetadata::resume);
      return Command.of(statement);
    } else if (queryId.get().toString().toLowerCase()
        .contains(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT)) {
      return Command.of(statement);
    }

    final PersistentQueryMetadata queryMetadata = context.getPersistentQuery(queryId.get())
        .orElseThrow(() -> new KsqlStatementException(
            "Unknown queryId: " + queryId.get(),
            statement.getMaskedStatementText()));

    if (queryMetadata.getPersistentQueryType() == KsqlConstants.PersistentQueryType.CREATE_SOURCE) {
      throw new KsqlStatementException(
          String.format("Cannot resume query '%s' because it is linked to a source table.",
              queryId.get()), statement.getMaskedStatementText());
    }

    queryMetadata.resume();
    return Command.of(statement);
  }

  private static Command createForTerminateQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext context
  ) {
    final TerminateQuery terminateQuery = (TerminateQuery) statement.getStatement();
    final Optional<QueryId> queryId = terminateQuery.getQueryId();

    if (!queryId.isPresent()) {
      context.getPersistentQueries().forEach(PersistentQueryMetadata::close);
      return Command.of(statement);
    } else if (queryId.get().toString().toLowerCase()
        .contains(KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT)) {
      return Command.of(statement);
    }

    final PersistentQueryMetadata queryMetadata = context.getPersistentQuery(queryId.get())
        .orElseThrow(() -> new KsqlStatementException(
            "Unknown queryId: " + queryId.get(),
            statement.getMaskedStatementText()));

    if (queryMetadata.getPersistentQueryType() == KsqlConstants.PersistentQueryType.CREATE_SOURCE) {
      throw new KsqlStatementException(
          String.format("Cannot terminate query '%s' because it is linked to a source table.",
              queryId.get()), statement.getMaskedStatementText());
    }

    queryMetadata.close();
    return Command.of(statement);
  }

  public static Command createForTerminateCluster(final ConfiguredStatement statement) {
    return Command.of(statement);
  }

  private static Command createForPlannedQuery(
      final ConfiguredStatement<? extends Statement> statement,
      final ServiceContext serviceContext,
      final KsqlExecutionContext context
  ) {
    final KsqlPlan plan = context.plan(serviceContext, statement);

    ConfiguredKsqlPlan configuredPlan = ConfiguredKsqlPlan
        .of(plan, statement.getSessionConfig());

    final KsqlExecutionContext.ExecuteResult result = context
        .execute(serviceContext, configuredPlan);
    if (result.getQuery().isPresent()
        && result.getQuery().get() instanceof PersistentQueryMetadataImpl
        && configuredPlan.getConfig()
          .getConfig(false)
          .getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)) {
      configuredPlan = ConfiguredKsqlPlan.of(
          plan,
          statement.getSessionConfig()
              .copyWith(ImmutableMap.of(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, false))
      );
    }
    return Command.of(configuredPlan);
  }
}
