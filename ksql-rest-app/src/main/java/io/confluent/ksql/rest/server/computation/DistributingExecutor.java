/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.execution.StatementExecutor;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.TopicInjector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@code StatementExecutor} that encapsulates a command queue and will
 * enqueue messages for asynchronous execution. It will optionally wait a
 * duration for the command to be executed remotely if configured with a
 * {@code distributedCmdResponseTimeout}.
 */
public class DistributingExecutor implements StatementExecutor<Statement> {

  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final Function<ServiceContext, SchemaInjector> schemaInjectorFactory;
  private final Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory;

  public DistributingExecutor(
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final Function<ServiceContext, SchemaInjector> schemaInjectorFactory,
      final Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory) {
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.schemaInjectorFactory =
        Objects.requireNonNull(schemaInjectorFactory, "schemaInjectorFactory");
    this.topicInjectorFactory = Objects
        .requireNonNull(topicInjectorFactory, "topicInjectorFactory");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
  }

  @Override
  public Optional<KsqlEntity> execute(
      final PreparedStatement<Statement> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    final PreparedStatement<?> withSchema = schemaInjectorFactory
        .apply(serviceContext).forStatement(statement);
    final PreparedStatement<?> withTopic = topicInjectorFactory
        .apply(executionContext).forStatement(withSchema, ksqlConfig, propertyOverrides);

    try {
      final QueuedCommandStatus queuedCommandStatus = commandQueue
          .enqueueCommand(withTopic, ksqlConfig, propertyOverrides);

      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return Optional.of(new CommandStatusEntity(
          withTopic.getStatementText(),
          queuedCommandStatus.getCommandId(),
          commandStatus,
          queuedCommandStatus.getCommandSequenceNumber()
      ));
    } catch (final Exception e) {
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic: " + e.getMessage(),
          statement.getStatementText()), e);
    }
  }
}
