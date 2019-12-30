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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
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
      CreateSourceProperties.from(ImmutableMap.of(
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic"),
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("json")
      ))
  );
  private static final ConfiguredStatement<Statement> CONFIGURED_STATEMENT =
      ConfiguredStatement.of(
          PreparedStatement.of("statement", STATEMENT),
          ImmutableMap.of(),
          KSQL_CONFIG
      );
  private static final CommandIdAssigner IDGEN = new CommandIdAssigner();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

  private DistributingExecutor distributor;
  private AtomicLong scnCounter;

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
    serviceContext = SandboxedServiceContext.create(TestServiceContext.create());
    when(executionContext.getServiceContext()).thenReturn(serviceContext);
    when(validatedCommandFactory.create(any(), any())).thenReturn(command);
    when(queue.createTransactionalProducer()).thenReturn(transactionalProducer);

    distributor = new DistributingExecutor(
        queue,
        DURATION_10_MS,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        Optional.of(authorizationValidator),
        validatedCommandFactory
    );
  }

  @Test
  public void shouldEnqueueSuccessfulCommandTransactionally() {
    // When:
    distributor.execute(CONFIGURED_STATEMENT, executionContext, serviceContext);

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
  public void shouldInferSchemas() {
    // When:
    distributor.execute(CONFIGURED_STATEMENT, executionContext, serviceContext);

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
            serviceContext
        )
            .orElseThrow(null);

    // Then:
    assertThat(commandStatusEntity,
        equalTo(new CommandStatusEntity("", CS_COMMAND, SUCCESS_STATUS, 1L)));
  }

  @Test
  public void shouldThrowExceptionOnFailureToEnqueue() {
    // Given:
    final KsqlException cause = new KsqlException("fail");

    when(queue.enqueueCommand(any(), any(), any())).thenThrow(cause);
    // Expect:
    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "Could not write the statement 'statement' into the command topic: fail");
    expectedException.expectCause(is(cause));

    // When:
    distributor.execute(CONFIGURED_STATEMENT, executionContext, serviceContext);
    verify(transactionalProducer, times(1)).abortTransaction();
  }

  @Test
  public void shouldThrowFailureIfCannotInferSchema() {
    // Given:
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, ImmutableMap.of(), KSQL_CONFIG);
    when(schemaInjector.inject(any())).thenThrow(new KsqlException("Could not infer!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not infer!");

    // When:
    distributor.execute(configured, executionContext, serviceContext);
  }

  @Test
  public void shouldThrowExceptionIfUserServiceContextIsDeniedAuthorization() {
    // Given:
    final ServiceContext userServiceContext = mock(ServiceContext.class);
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, ImmutableMap.of(), KSQL_CONFIG);
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(eq(userServiceContext), any(), eq(configured.getStatement()));

    // Expect:
    expectedException.expect(KsqlTopicAuthorizationException.class);

    // When:
    distributor.execute(configured, executionContext, userServiceContext);
  }

  @Test
  public void shouldThrowServerExceptionIfServerServiceContextIsDeniedAuthorization() {
    // Given:
    final ServiceContext userServiceContext = SandboxedServiceContext.create(TestServiceContext.create());
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, ImmutableMap.of(), KSQL_CONFIG);
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(eq(serviceContext), any(), eq(configured.getStatement()));

    // Expect:
    expectedException.expect(KsqlServerException.class);
    expectedException.expectCause(is(instanceOf(KsqlTopicAuthorizationException.class)));

    // When:
    distributor.execute(configured, executionContext, userServiceContext);
  }
}
