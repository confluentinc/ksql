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
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.execution.StatementExecutor;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@code StatementExecutor} that encapsulates a command queue and will
 * enqueue messages for asynchronous execution. It will optionally wait a
 * duration for the command to be executed remotely if configured with a
 * {@code distributedCmdResponseTimeout}.
 */
public class DistributingExecutor implements StatementExecutor  {

  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final Function<ServiceContext, SchemaInjector> schemaInjectorFactory;

  public DistributingExecutor(
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final Function<ServiceContext, SchemaInjector> schemaInjectorFactory) {
    this.commandQueue = commandQueue;
    this.distributedCmdResponseTimeout = distributedCmdResponseTimeout;
    this.schemaInjectorFactory = schemaInjectorFactory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<KsqlEntity> execute(
      final PreparedStatement statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides) {
    final PreparedStatement withSchema =
        schemaInjectorFactory.apply(serviceContext).forStatement(statement);

    try {
      final QueuedCommandStatus queuedCommandStatus = commandQueue
          .enqueueCommand(withSchema, ksqlConfig, propertyOverrides);

      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return Optional.of(new CommandStatusEntity(
          withSchema.getStatementText(),
          queuedCommandStatus.getCommandId(),
          commandStatus,
          queuedCommandStatus.getCommandSequenceNumber()
      ));
    } catch (final Exception e) {
      throw new KsqlException(String.format(
          "Could not write the statement '%s' into the command " + "topic.",
          statement.getStatementText()), e);
    }
  }
}
