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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.engine.rewrite.DataSourceExtractor;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.execution.StatementExecutorResponse;
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
import java.util.ArrayList;
import java.util.List;
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
  private final Supplier<String> commandRunnerWarning;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public DistributingExecutor(
      final KsqlConfig ksqlConfig,
      final CommandQueue commandQueue,
      final Duration distributedCmdResponseTimeout,
      final BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
      final Optional<KsqlAuthorizationValidator> authorizationValidator,
      final ValidatedCommandFactory validatedCommandFactory,
      final Errors errorHandler,
      final Supplier<String> commandRunnerWarning
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
    this.commandRunnerWarning =
        Objects.requireNonNull(commandRunnerWarning, "commandRunnerWarning");
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private Optional<StatementExecutorResponse> checkIfNotExistsResponse(
      final KsqlExecutionContext executionContext,
      final ConfiguredStatement<?> statement
  ) {
    SourceName sourceName = null;
    String type = "";
    if (statement.getStatement() instanceof CreateStream
        && ((CreateStream) statement.getStatement()).isNotExists()) {
      type = "stream";
      sourceName = ((CreateStream) statement.getStatement()).getName();
    } else if (statement.getStatement() instanceof CreateTable
        && ((CreateTable) statement.getStatement()).isNotExists()) {
      type = "table";
      sourceName = ((CreateTable) statement.getStatement()).getName();
    } else if (statement.getStatement() instanceof CreateTableAsSelect
        && ((CreateTableAsSelect) statement.getStatement()).isNotExists()) {
      type = "table";
      sourceName = ((CreateTableAsSelect) statement.getStatement()).getName();
    } else if (statement.getStatement() instanceof CreateStreamAsSelect
        && ((CreateStreamAsSelect) statement.getStatement()).isNotExists()) {
      type = "stream";
      sourceName = ((CreateStreamAsSelect) statement.getStatement()).getName();
    }
    if (sourceName != null
        && executionContext.getMetaStore().getSource(sourceName) != null) {
      return Optional.of(StatementExecutorResponse.handled(Optional.of(
          new WarningEntity(statement.getStatementText(),
              String.format("Cannot add %s %s: A %s with the same name already exists.",
                  type,
                  sourceName,
                  type)
          ))));
    } else {
      return Optional.empty();
    }
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
  // CHECKSTYLE_RULES.OFF: NPathComplexity
  public StatementExecutorResponse execute(
      final ConfiguredStatement<? extends Statement> statement,
      final KsqlExecutionContext executionContext,
      final KsqlSecurityContext securityContext
  ) {
    final String commandRunnerWarningString = commandRunnerWarning.get();
    if (!commandRunnerWarningString.equals("")) {
      throw new KsqlServerException("Failed to handle Ksql Statement."
          + System.lineSeparator()
          + commandRunnerWarningString);
    }
    final ConfiguredStatement<?> injected = injectorFactory
        .apply(executionContext, securityContext.getServiceContext())
        .inject(statement);

    if (injected.getStatement() instanceof InsertInto) {
      validateInsertIntoQueries(
          executionContext.getMetaStore(),
          (InsertInto) injected.getStatement()
      );
    }

    final Optional<StatementExecutorResponse> response = checkIfNotExistsResponse(
        executionContext,
        statement
    );

    if (response.isPresent()) {
      return response.get();
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

    CommandId commandId = null;
    try {
      transactionalProducer.beginTransaction();
      commandQueue.waitForCommandConsumer();

      commandId = commandIdAssigner.getCommandId(statement.getStatement());
      final Command command = validatedCommandFactory.create(
          injected,
          executionContext.createSandbox(executionContext.getServiceContext())
      );
      final QueuedCommandStatus queuedCommandStatus =
          commandQueue.enqueueCommand(commandId, command, transactionalProducer);

      transactionalProducer.commitTransaction();
      final CommandStatus commandStatus = queuedCommandStatus
          .tryWaitForFinalStatus(distributedCmdResponseTimeout);

      return StatementExecutorResponse.handled(Optional.of(new CommandStatusEntity(
          injected.getStatementText(),
          queuedCommandStatus.getCommandId(),
          commandStatus,
          queuedCommandStatus.getCommandSequenceNumber(),
          getDeprecatedWarnings(executionContext.getMetaStore(), injected)
      )));
    } catch (final ProducerFencedException
        | OutOfOrderSequenceException
        | AuthorizationException e
    ) {
      // We can't recover from these exceptions, so our only option is close producer and exit.
      // This catch doesn't abortTransaction() since doing that would throw another exception.
      if (commandId != null) {
        commandQueue.abortCommand(commandId);
      }
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic.",
          statement.getStatementText()), e);
    } catch (final Exception e) {
      transactionalProducer.abortTransaction();
      if (commandId != null) {
        commandQueue.abortCommand(commandId);
      }
      throw new KsqlServerException(String.format(
          "Could not write the statement '%s' into the command topic.",
          statement.getStatementText()), e);
    } finally {
      transactionalProducer.close();
    }
  }

  /**
   * @return a list of warning messages for deprecated syntax statements
   */
  private List<KsqlWarning> getDeprecatedWarnings(
      final MetaStore metaStore,
      final ConfiguredStatement<?> statement
  ) {
    final List<KsqlWarning> deprecatedWarnings = new ArrayList<>();

    if (isLeftOrOuterStreamStreamJoinWithoutGrace(metaStore, statement.getStatement())) {
      deprecatedWarnings.add(new KsqlWarning(
          "DEPRECATION NOTICE: Left/Outer stream-stream joins statements without a GRACE PERIOD "
              + "will not be accepted in a future ksqlDB version.\n"
              + "Please use the GRACE PERIOD clause as specified in "
              + "https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/"
              + "select-push-query/"
      ));
    }

    return deprecatedWarnings;
  }

  private boolean isLeftOrOuterStreamStreamJoinWithoutGrace(
      final MetaStore metaStore,
      final Statement statement
  ) {
    // Check only CREATE_AS statements.
    // Pull and push queries are not written in the command topic.
    if (!(statement instanceof CreateAsSelect)) {
      return false;
    }

    if (((CreateAsSelect) statement).getQuery().getFrom() instanceof Join) {
      final Join join = (Join) ((CreateAsSelect) statement).getQuery().getFrom();

      // Check join sources are streams only
      final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore, false);
      dataSourceExtractor.extractDataSources(((CreateAsSelect) statement).getQuery());
      for (final Analysis.AliasedDataSource dataSource : dataSourceExtractor.getAllSources()) {
        if (dataSource.getDataSource().getDataSourceType() != DataSource.DataSourceType.KSTREAM) {
          return false;
        }
      }

      // Check for left/outer join types and a grace period clause
      for (final JoinedSource joinedSource : join.getRights()) {
        if (joinedSource.getType() == JoinedSource.Type.LEFT
            || joinedSource.getType() == JoinedSource.Type.OUTER) {
          if (!joinedSource.getWithinExpression().flatMap(WithinExpression::getGrace).isPresent()) {
            return true;
          }
        }
      }
    }

    return false;
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

  private void validateInsertIntoQueries(
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

    if (!dataSource.getSchema().headers().isEmpty()) {
      throw new KsqlException("Cannot insert into " + insertInto.getTarget().text()
          + " because it has header columns");
    }
  }
}
