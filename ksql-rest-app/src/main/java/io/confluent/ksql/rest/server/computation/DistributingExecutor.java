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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.execution.StatementExecutor;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * A {@code StatementExecutor} that encapsulates a command queue and will
 * enqueue messages for asynchronous execution. It will optionally wait a
 * duration for the command to be executed remotely if configured with a
 * {@code distributedCmdResponseTimeout}.
 */
public class DistributingExecutor implements StatementExecutor<Statement> {

  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;

  public DistributingExecutor(
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Optional<KsqlAuthorizationValidator> authorizationValidator
  ) {
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
    this.authorizationValidator =
        Objects.requireNonNull(authorizationValidator, "authorizationValidator");
  }

  @Override
  public Optional<KsqlEntity> execute(
      final ConfiguredStatement<Statement> statement,
      final Map<String, Object> mutableScopedProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext) {
    final ConfiguredStatement<?> injected = injectorFactory
        .apply(executionContext, serviceContext)
        .inject(statement);

    checkAuthorization(injected, serviceContext, executionContext);

    try {
      final QueuedCommandStatus queuedCommandStatus = commandQueue.enqueueCommand(injected);
      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return Optional.of(new CommandStatusEntity(
          injected.getStatementText(),
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

  private void checkAuthorization(
      final ConfiguredStatement<?> configured,
      final ServiceContext userServiceContext,
      final KsqlExecutionContext serverExecutionContext
  ) {
    final Statement statement = configured.getStatement();
    final MetaStore metaStore = serverExecutionContext.getMetaStore();

    // Check the User will be permitted to execute this statement
    authorizationValidator.ifPresent(
        validator ->
            validator.checkAuthorization(userServiceContext, metaStore, statement));

    try {
      // Check the KSQL service principal will be permitted too
      authorizationValidator.ifPresent(
          validator -> validator.checkAuthorization(
              serverExecutionContext.getServiceContext(),
              metaStore,
              statement));
    } catch (final Exception e) {
      throw new KsqlServerException("The KSQL server is not permitted to execute the command", e);
    }
  }
}
