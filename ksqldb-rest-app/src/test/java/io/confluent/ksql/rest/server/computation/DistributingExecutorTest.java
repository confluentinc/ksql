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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.execution.StatementExecutorResponse;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DistributingExecutorTest {

  private static final Duration DURATION_10_MS = Duration.ofMillis(10);
  private static final CommandId CS_COMMAND = new CommandId(Type.STREAM, "stream", Action.CREATE);
  private static final CommandStatus SUCCESS_STATUS = new CommandStatus(Status.SUCCESS, "");
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(new HashMap<>());
  private static final Statement STATEMENT = new CreateStream(
      SourceName.of("TEST"),
      TableElements.of(),
      false,
      false,
      CreateSourceProperties.from(ImmutableMap.of(
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"),
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("json")
      )),
      false
  );
  private static final ConfiguredStatement<Statement> CONFIGURED_STATEMENT =
      ConfiguredStatement.of(PreparedStatement.of("statement", STATEMENT),
          SessionConfig.of(KSQL_CONFIG, ImmutableMap.of()));
  private static final CommandIdAssigner IDGEN = new CommandIdAssigner();

  @Mock
  private CommandQueue queue;
  @Mock
  private QueuedCommandStatus status;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector topicInjector;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private KsqlExecutionContext sandboxContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private ValidatedCommandFactory validatedCommandFactory;
  @Mock
  private Producer<CommandId, Command> transactionalProducer;
  @Mock
  private Command command;
  @Mock
  private Errors errorHandler;
  @Mock
  private Supplier<String> commandRunnerWarning;

  private DistributingExecutor distributor;
  private AtomicLong scnCounter;
  private KsqlSecurityContext securityContext;

  @Before
  public void setUp() throws InterruptedException {
    scnCounter = new AtomicLong();
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(queue.enqueueCommand(any(), any(), any())).thenReturn(status);
    when(status.tryWaitForFinalStatus(any())).thenReturn(SUCCESS_STATUS);
    when(status.getCommandId()).thenReturn(CS_COMMAND);
    when(status.getCommandSequenceNumber()).thenAnswer(inv -> scnCounter.incrementAndGet());
    when(executionContext.getMetaStore()).thenReturn(metaStore);
    when(executionContext.createSandbox(any())).thenReturn(sandboxContext);
    when(commandRunnerWarning.get()).thenReturn("");
    serviceContext = SandboxedServiceContext.create(TestServiceContext.create());
    when(executionContext.getServiceContext()).thenReturn(serviceContext);
    when(validatedCommandFactory.create(any(), any())).thenReturn(command);
    when(queue.createTransactionalProducer()).thenReturn(transactionalProducer);

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    distributor = new DistributingExecutor(
        KSQL_CONFIG,
        queue,
        DURATION_10_MS,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        Optional.of(authorizationValidator),
        validatedCommandFactory,
        errorHandler,
        commandRunnerWarning
    );
  }

  @Test
  public void shouldEnqueueSuccessfulCommandTransactionally() {
    // When:
    distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);

    // Then:
    final InOrder inOrder = Mockito.inOrder(transactionalProducer, queue, validatedCommandFactory);
    inOrder.verify(transactionalProducer).initTransactions();
    inOrder.verify(transactionalProducer).beginTransaction();
    inOrder.verify(queue).waitForCommandConsumer();
    inOrder.verify(validatedCommandFactory).create(
        CONFIGURED_STATEMENT,
        sandboxContext
    );
    inOrder.verify(queue).enqueueCommand(
        IDGEN.getCommandId(CONFIGURED_STATEMENT.getStatement()),
        command,
        transactionalProducer
    );
    inOrder.verify(transactionalProducer).commitTransaction();
    inOrder.verify(transactionalProducer).close();
  }

  @Test
  public void shouldNotAbortTransactionIfInitTransactionFails() {
    // Given:
    doThrow(TimeoutException.class).when(transactionalProducer).initTransactions();

    // When:
    assertThrows(
        KsqlServerException.class,
        () -> distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext)
    );
    verify(transactionalProducer, times(0)).abortTransaction();
  }

  @Test
  public void shouldInferSchemas() {
    // When:
    distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);

    // Then:
    verify(schemaInjector, times(1)).inject(eq(CONFIGURED_STATEMENT));
  }

  @Test
  public void shouldReturnCommandStatus() {
    // When:
    final CommandStatusEntity commandStatusEntity =
        (CommandStatusEntity) distributor.execute(
            CONFIGURED_STATEMENT,
            executionContext,
            securityContext
        )
            .getEntity()
            .orElseThrow(null);

    // Then:
    assertThat(commandStatusEntity,
        equalTo(new CommandStatusEntity("", CS_COMMAND, SUCCESS_STATUS, 1L)));
  }

  @Test
  public void shouldNotInitTransactionWhenCommandRunnerWarningPresent() {
    // When:
    when(commandRunnerWarning.get()).thenReturn(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE);

    // Then:
    assertThrows(
        KsqlServerException.class,
        () -> distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext)
    );
    verify(transactionalProducer, never()).initTransactions();
  }

  @Test
  public void shouldThrowExceptionOnFailureToEnqueue() {
    // Given:
    final KsqlException cause = new KsqlException("fail");

    when(queue.enqueueCommand(any(), any(), any())).thenThrow(cause);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not write the statement into the command topic."));
    assertThat(e.getUnloggedMessage(), containsString(
        "Could not write the statement 'statement' into the command topic."));
    assertThat(e.getSqlStatement(), containsString("statement"));
    assertThat(e.getCause(), (is(cause)));
    verify(transactionalProducer, times(1)).abortTransaction();
  }

  @Test
  public void shouldThrowFailureIfCannotInferSchema() {
    // Given:
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    when(schemaInjector.inject(any())).thenThrow(new KsqlException("Could not infer!"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> distributor.execute(configured, executionContext, securityContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not infer!"));
  }

  @Test
  public void shouldThrowExceptionIfUserServiceContextIsDeniedAuthorization() {
    // Given:
    final KsqlSecurityContext userSecurityContext = new KsqlSecurityContext(
        Optional.empty(),
        mock(ServiceContext.class));
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(eq(userSecurityContext), any(), eq(configured.getStatement()));

    // When:
    assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> distributor.execute(configured, executionContext, userSecurityContext)
    );
  }

  @Test
  public void shouldThrowServerExceptionIfServerServiceContextIsDeniedAuthorization() {
    // Given:
    final KsqlSecurityContext userSecurityContext = new KsqlSecurityContext(Optional.empty(),
        SandboxedServiceContext.create(TestServiceContext.create()));
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    doNothing().when(authorizationValidator)
        .checkAuthorization(eq(userSecurityContext), any(), any());
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(
            ArgumentMatchers.argThat(securityContext ->
                securityContext.getServiceContext() == serviceContext),
            any(), any());

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> distributor.execute(configured, executionContext, userSecurityContext)
    );

    // Then:
    assertThat(e.getCause(), (is(instanceOf(KsqlTopicAuthorizationException.class))));
  }

  @Test
  public void shouldThrowExceptionWhenInsertIntoUnknownStream() {
    // Given
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new InsertInto(SourceName.of("s1"), mock(Query.class)));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    doReturn(null).when(metaStore).getSource(SourceName.of("s1"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> distributor.execute(configured, executionContext, mock(KsqlSecurityContext.class))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert into an unknown stream/table: `s1`"));
  }

  @Test
  public void shouldThrowExceptionWhenInsertIntoReadOnlyTopic() {
    // Given
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new InsertInto(SourceName.of("s1"), mock(Query.class)));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    final DataSource dataSource = mock(DataSource.class);
    doReturn(dataSource).when(metaStore).getSource(SourceName.of("s1"));
    when(dataSource.getKafkaTopicName()).thenReturn("_confluent-ksql-default__command-topic");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> distributor.execute(configured, executionContext, mock(KsqlSecurityContext.class))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert into read-only topic: "
            + "_confluent-ksql-default__command-topic"));
  }

  @Test
  public void shouldThrowExceptionWhenInsertIntoProcessingLogTopic() {
    // Given
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new InsertInto(SourceName.of("s1"), mock(Query.class)));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    final DataSource dataSource = mock(DataSource.class);
    doReturn(dataSource).when(metaStore).getSource(SourceName.of("s1"));
    when(dataSource.getKafkaTopicName()).thenReturn("default_ksql_processing_log");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> distributor.execute(configured, executionContext, mock(KsqlSecurityContext.class))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot insert into read-only topic: "
            + "default_ksql_processing_log"));
  }

  @Test
  public void shouldThrowExceptionWhenInsertIntoSourceWithHeaders() {
    // Given
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new InsertInto(SourceName.of("s1"), mock(Query.class)));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    final DataSource dataSource = mock(DataSource.class);
    final LogicalSchema schema = mock(LogicalSchema.class);
    doReturn(dataSource).when(metaStore).getSource(SourceName.of("s1"));
    doReturn(schema).when(dataSource).getSchema();
    doReturn(ImmutableList.of(ColumnName.of("a"))).when(schema).headers();
    when(dataSource.getKafkaTopicName()).thenReturn("topic");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> distributor.execute(configured, executionContext, mock(KsqlSecurityContext.class))
    );

    // Then:
    assertThat(e.getMessage(), is("Cannot insert into s1 because it has header columns"));
  }

  @Test
  public void shouldAbortOnError_ProducerFencedException() {
    // When:
    doThrow(new ProducerFencedException("Error!")).when(transactionalProducer).commitTransaction();
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext)
    );

    assertThat(e.getMessage(), containsString("Could not write the statement into the command topic."));
    assertThat(e.getUnloggedMessage(), containsString("Could not write the statement "
        + "'statement' into the command topic."));
    assertThat(e.getSqlStatement(), containsString("statement"));

    // Then:
    verify(queue).abortCommand(IDGEN.getCommandId(CONFIGURED_STATEMENT.getStatement()));
  }

  @Test
  public void shouldAbortOnError_Exception() {
    // When:
    doThrow(new RuntimeException("Error!")).when(transactionalProducer).commitTransaction();
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> distributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext)
    );

    assertThat(e.getMessage(), containsString("Could not write the statement into the command topic."));
    assertThat(e.getUnloggedMessage(), containsString("Could not write the statement "
        + "'statement' into the command topic."));
    assertThat(e.getSqlStatement(), containsString("statement"));

    // Then:
    verify(queue).abortCommand(IDGEN.getCommandId(CONFIGURED_STATEMENT.getStatement()));
  }

  @Test
  public void shouldNotEnqueueRedundantIfNotExists() {
    // Given:
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new CreateStream(
            SourceName.of("TEST"),
            TableElements.of(),
            false,
            true,
            CreateSourceProperties.from(ImmutableMap.of(
                CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"),
                CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("json")
            )),
            false
        ));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of())
        );
    final DataSource dataSource = mock(DataSource.class);
    doReturn(dataSource).when(metaStore).getSource(SourceName.of("TEST"));

    // When:
    final StatementExecutorResponse response = distributor.execute(configured, executionContext, securityContext);

    // Then:
    assertThat("Should be present", response.getEntity().isPresent());
    assertThat(((WarningEntity) response.getEntity().get()).getMessage(), containsString(""));
  }
  
  @Test
  public void shouldThrowIfRateLimitHit() {
    // Given:
    final DistributingExecutor rateLimitedDistributor = new DistributingExecutor(
      new KsqlConfig(ImmutableMap.of(KsqlRestConfig.KSQL_COMMAND_TOPIC_RATE_LIMIT_CONFIG, 0.25)),
      queue,
      DURATION_10_MS,
      (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
      Optional.of(authorizationValidator),
      validatedCommandFactory,
      errorHandler,
      commandRunnerWarning
    );
    
    // When:
    rateLimitedDistributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);


    // Then:
    boolean exceptionFound = false;
    try {
      rateLimitedDistributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);
      rateLimitedDistributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);
      rateLimitedDistributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);
      rateLimitedDistributor.execute(CONFIGURED_STATEMENT, executionContext, securityContext);
    } catch (Exception e) {
      assertTrue(e instanceof KsqlRestException);
      final KsqlRestException restException = (KsqlRestException) e; 
      assertEquals(restException.getResponse().getStatus(), 429);
      final KsqlErrorMessage errorMessage = (KsqlErrorMessage) restException.getResponse().getEntity();
      assertTrue(errorMessage.getMessage().contains("DDL/DML rate is crossing the configured rate limit of statements/second"));
      exceptionFound = true;
    }
    assertTrue(exceptionFound);
  }
}
