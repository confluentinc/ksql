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
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;

/**
 * A {@code StatementExecutor} that encapsulates a command queue and will
 * enqueue messages for asynchronous execution. It will optionally wait a
 * duration for the command to be executed remotely if configured with a
 * {@code distributedCmdResponseTimeout}.
 */
public class DistributingExecutor {
  private final CommandQueue commandQueue;
  private final Duration distributedCmdResponseTimeout;
  private final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  private final Optional<KsqlAuthorizationValidator> authorizationValidator;
  private final ValidatedCommandFactory validatedCommandFactory;
  private final CommandIdAssigner commandIdAssigner;
  private final ReservedInternalTopics internalTopics;
  private final Errors errorHandler;
  private final Supplier<Boolean> commandRunnerDegraded;

  public DistributingExecutor(
      final KsqlConfig ksqlConfig,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final ValidatedCommandFactory validatedCommandFactory,
      final Errors errorHandler,
      final Supplier<Boolean> commandRunnerDegraded
  ) {
    this.commandQueue = commandQueue;
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
    this.authorizationValidator =
        Objects.requireNonNull(authorizationValidator, "authorizationValidator");
    this.validatedCommandFactory = Objects.requireNonNull(
        validatedCommandFactory,
        "validatedCommandFactory"
    );
    this.commandIdAssigner = new CommandIdAssigner();
    this.internalTopics =
        new ReservedInternalTopics(Objects.requireNonNull(ksqlConfig, "ksqlConfig"));
    this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
    this.commandRunnerDegraded =
        Objects.requireNonNull(commandRunnerDegraded, "commandRunnerDegraded");
  }

  /**
   * The transactional protocol for sending a command to the command topic is to
   * initTransaction(), beginTransaction(), wait for commandRunner to finish processing all previous
   * commands that were present at the start of the transaction, validate the current command,
   * enqueue the command in the command topic, and commit the transaction.
   * Only successfully committed commands can be read by the command topic consumer.
   * If any exceptions are thrown during this protocol, the transaction is aborted.
   * If a new transactional producer is initialized while the current transaction is incomplete,
   * the old producer will be fenced off and unable to continue with its transaction.
   */
  public Optional<KsqlEntity> execute(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext executionContext,
      final KsqlSecurityContext securityContext
  ) {
    if (commandRunnerDegraded.get()) {
      throw new KsqlServerException("Failed to handle Ksql Statement."
          + System.lineSeparator()
          + errorHandler.commandRunnerDegradedErrorMessage());
    }
    final ConfiguredStatement<?> injected = injectorFactory
        .apply(executionContext, securityContext.getServiceContext())
        .inject(statement);

    if (injected.getStatement() instanceof InsertInto) {
      throwIfInsertOnReadOnlyTopic(
          executionContext.getMetaStore(),
          (InsertInto)injected.getStatement()
      );
    }

    checkAuthorization(injected, securityContext, executionContext);

    final Producer<CommandId, Command> transactionalProducer =
        commandQueue.createTransactionalProducer();

    try {
      transactionalProducer.initTransactions();
    } catch (final TimeoutException e) {
      throw new KsqlServerException(errorHandler.transactionInitTimeoutErrorMessage(e), e);
    } catch (final Exception e) {
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic: " + e.getMessage(),
          statement.getStatementText()), e);
    }
    
    try {
      transactionalProducer.beginTransaction();
      commandQueue.waitForCommandConsumer();

      final CommandId commandId = commandIdAssigner.getCommandId(statement.getStatement());
      final Command command = validatedCommandFactory.create(
          injected,
          executionContext.createSandbox(executionContext.getServiceContext())
      );
      final QueuedCommandStatus queuedCommandStatus =
          commandQueue.enqueueCommand(commandId, command, transactionalProducer);

      transactionalProducer.commitTransaction();
      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return Optional.of(new CommandStatusEntity(
          injected.getStatementText(),
          queuedCommandStatus.getCommandId(),
          commandStatus,
          queuedCommandStatus.getCommandSequenceNumber()
      ));
    } catch (final ProducerFencedException
        | OutOfOrderSequenceException
        | AuthorizationException e
    ) {
      // We can't recover from these exceptions, so our only option is close producer and exit.
      // This catch doesn't abortTransaction() since doing that would throw another exception.
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic.",
          statement.getStatementText()), e);
    } catch (final Exception e) {
      transactionalProducer.abortTransaction();
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic.",
          statement.getStatementText()), e);
    } finally {
      transactionalProducer.close();
    }
  }

  private void checkAuthorization(
      final ConfiguredStatement<?> configured,
      final KsqlSecurityContext userSecurityContext,
      final KsqlExecutionContext serverExecutionContext
  ) {
    final Statement statement = configured.getStatement();
    final MetaStore metaStore = serverExecutionContext.getMetaStore();

    // Check the User will be permitted to execute this statement
    authorizationValidator.ifPresent(
        validator ->
            validator.checkAuthorization(userSecurityContext, metaStore, statement));

    try {
      // Check the KSQL service principal will be permitted too
      authorizationValidator.ifPresent(
          validator -> validator.checkAuthorization(
              new KsqlSecurityContext(Optional.empty(), serverExecutionContext.getServiceContext()),
              metaStore,
              statement));
    } catch (final Exception e) {
      throw new KsqlServerException("The KSQL server is not permitted to execute the command", e);
    }
  }

  private void throwIfInsertOnReadOnlyTopic(
      final MetaStore metaStore,
      final InsertInto insertInto
  ) {
    final DataSource dataSource = metaStore.getSource(insertInto.getTarget());
    if (dataSource == null) {
      throw new KsqlException("Cannot insert into an unknown stream/table: "
          + insertInto.getTarget());
    }

    if (internalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlException("Cannot insert into read-only topic: "
          + dataSource.getKafkaTopicName());
    }
  }
}
