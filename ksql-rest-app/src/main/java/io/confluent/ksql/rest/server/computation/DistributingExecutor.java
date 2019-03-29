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
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.execution.StatementExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
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
  private final Function<ServiceContext, Injector> schemaInjectorFactory;
  private final Function<KsqlExecutionContext, Injector> topicInjectorFactory;

  public DistributingExecutor(
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final Function<ServiceContext, Injector> schemaInjectorFactory,
      final Function<KsqlExecutionContext, Injector> topicInjectorFactory) {
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
      final ConfiguredStatement<Statement> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ConfiguredStatement<?> withSchema = schemaInjectorFactory
        .apply(serviceContext)
        .inject(statement);
    final ConfiguredStatement<?> withTopic = topicInjectorFactory
        .apply(executionContext)
        .inject(withSchema);

    try {
      final QueuedCommandStatus queuedCommandStatus = commandQueue.enqueueCommand(withTopic);
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
