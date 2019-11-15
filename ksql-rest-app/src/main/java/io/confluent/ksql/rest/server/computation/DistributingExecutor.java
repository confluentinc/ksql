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
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.validation.RequestValidator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

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
  private final KsqlAuthorizationValidator authorizationValidator;
  private final RequestValidator requestValidator;

  public DistributingExecutor(
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final KsqlAuthorizationValidator authorizationValidator,
      final RequestValidator requestValidator
  ) {
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.distributedCmdResponseTimeout =
        Objects.requireNonNull(distributedCmdResponseTimeout, "distributedCmdResponseTimeout");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
    this.authorizationValidator =
        Objects.requireNonNull(authorizationValidator, "authorizationValidator");
    this.requestValidator =
        Objects.requireNonNull(requestValidator, "requestValidator");
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
      final ConfiguredStatement<Statement> statement,
      final ParsedStatement parsedStatement,
      final Map<String, Object> mutableScopedProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ConfiguredStatement<?> injected = injectorFactory
        .apply(executionContext, serviceContext)
        .inject(statement);

    checkAuthorization(injected, serviceContext, executionContext);

    final Producer<CommandId, Command> transactionalProducer =
        commandQueue.createTransactionalProducer();
    try {
      transactionalProducer.initTransactions();
      transactionalProducer.beginTransaction();
      commandQueue.waitForCommandConsumer();
      
      // Don't perform validation on Terminate Cluster statements
      if (!parsedStatement.getStatementText()
          .equals(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT)) {
        requestValidator.validate(
            SandboxedServiceContext.create(serviceContext),
            Collections.singletonList(parsedStatement),
            mutableScopedProperties,
            parsedStatement.getStatementText()
        );
      }

      final QueuedCommandStatus queuedCommandStatus =
          commandQueue.enqueueCommand(injected, transactionalProducer);

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
          "Could not write the statement '%s' into the command topic: " + e.getMessage(),
          statement.getStatementText()), e);
    } catch (final Exception e) {
      transactionalProducer.abortTransaction();
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic: " + e.getMessage(),
          statement.getStatementText()), e);
    } finally {
      transactionalProducer.close();
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
    authorizationValidator.checkAuthorization(userServiceContext, metaStore, statement);

    try {
      // Check the KSQL service principal will be permitted too
      authorizationValidator.checkAuthorization(
          serverExecutionContext.getServiceContext(),
          metaStore,
          statement
      );
    } catch (final Exception e) {
      throw new KsqlServerException("The KSQL server is not permitted to execute the command", e);
    }
  }
}
