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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.validation.RequestValidator;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
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

  private static final String SQL_STRING = "some ksql statement;";
  private static final Duration DURATION_10_MS = Duration.ofMillis(10);
  private static final CommandId CS_COMMAND = new CommandId(Type.STREAM, "stream", Action.CREATE);
  private static final CommandStatus SUCCESS_STATUS = new CommandStatus(Status.SUCCESS, "");
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(new HashMap<>());
  private static final String ORIGINAL_SQL = "original-sql";
  private static final String INJECTED_SQL = "injected-sql";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private CommandQueue queue;
  @Mock
  private QueuedCommandStatus status;
  @Mock
  private SandboxedServiceContext userServiceContext;
  @Mock
  private ServiceContext serverServiceContext;
  @Mock
  private Injector injector;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private RequestValidator requestValidator;
  @Mock
  private Producer<CommandId, Command> transactionalProducer;
  @Mock
  private ConfiguredStatement<Statement> configuredStatement;
  @Mock
  private ConfiguredStatement<Statement> injectedStatement;
  @Mock
  private Statement statement;

  private DistributingExecutor distributor;
  private AtomicLong scnCounter;

  @Before
  public void setUp() throws InterruptedException {
    scnCounter = new AtomicLong();
    when(injector.inject(any())).thenReturn(injectedStatement);
    when(queue.enqueueCommand(injectedStatement, transactionalProducer)).thenReturn(status);
    when(status.tryWaitForFinalStatus(any())).thenReturn(SUCCESS_STATUS);
    when(status.getCommandId()).thenReturn(CS_COMMAND);
    when(status.getCommandSequenceNumber()).thenAnswer(inv -> scnCounter.incrementAndGet());
    when(executionContext.getMetaStore()).thenReturn(metaStore);
    when(executionContext.getServiceContext()).thenReturn(serverServiceContext);
    when(queue.createTransactionalProducer()).thenReturn(transactionalProducer);
    when(configuredStatement.getStatementText()).thenReturn(ORIGINAL_SQL);
    when(injectedStatement.getStatementText()).thenReturn(INJECTED_SQL);
    when(injectedStatement.getStatement()).thenReturn(statement);

    distributor = new DistributingExecutor(
        queue,
        DURATION_10_MS,
        (ec, sc) -> injector,
        Optional.of(authorizationValidator),
        requestValidator
    );
  }

  @Test
  public void shouldEnqueueSuccessfulCommandTransactionally() {
    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);

    // Then:
    final InOrder inOrder = Mockito.inOrder(transactionalProducer, queue, requestValidator);
    inOrder.verify(transactionalProducer, times(1)).initTransactions();
    inOrder.verify(transactionalProducer, times(1)).beginTransaction();
    inOrder.verify(queue, times(1)).waitForCommandConsumer();
    inOrder.verify(requestValidator).validate(
        any(),
        any(ConfiguredStatement.class),
        any(),
        any());
    inOrder.verify(queue, times(1)).enqueueCommand(any(), any());
    inOrder.verify(transactionalProducer, times(1)).commitTransaction();
    inOrder.verify(transactionalProducer, times(1)).close();
  }

  @Test
  public void shouldValidateStatements() {
    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);

    // Then:
    verify(requestValidator).validate(
        userServiceContext,
        injectedStatement,
        ImmutableMap.of(),
        INJECTED_SQL
    );
  }

  @Test
  public void shouldCallInjector() {
    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);

    // Then:
    verify(injector, times(1)).inject(eq(configuredStatement));
  }

  @Test
  public void shouldReturnCommandStatus() {
    // When:
    final List<? extends KsqlEntity> results = distributor.execute(
        configuredStatement,
        ImmutableMap.of(),
        executionContext,
        userServiceContext
    );

    // Then:
    assertThat(results,
        contains(new CommandStatusEntity("", CS_COMMAND, SUCCESS_STATUS, 1L)));
  }

  @Test
  public void shouldThrowExceptionOnFailureToEnqueue() {
    // Given:
    final KsqlException cause = new KsqlException("fail");
    when(queue.enqueueCommand(any(), any())).thenThrow(cause);

    // Expect:
    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "Could not write the statement '" + ORIGINAL_SQL + "' into the command topic: fail");
    expectedException.expectCause(is(cause));

    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);
    verify(transactionalProducer, times(1)).abortTransaction();
  }

  @Test
  public void shouldThrowFailureIfInject() {
    // Given:
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("", new ListProperties(Optional.empty()));
    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(preparedStatement, ImmutableMap.of(), KSQL_CONFIG);
    when(injector.inject(any())).thenThrow(new KsqlException("Could not infer!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not infer!");

    // When:
    distributor.execute(configured, ImmutableMap.of(), executionContext, userServiceContext);
  }

  @Test
  public void shouldThrowExceptionIfUserServiceContextIsDeniedAuthorization() {
    // Given:
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(eq(userServiceContext), any(), eq(statement));

    // Expect:
    expectedException.expect(KsqlTopicAuthorizationException.class);

    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);
  }

  @Test
  public void shouldThrowServerExceptionIfServerServiceContextIsDeniedAuthorization() {
    // Given:
    doThrow(KsqlTopicAuthorizationException.class).when(authorizationValidator)
        .checkAuthorization(eq(serverServiceContext), any(), eq(statement));

    // Expect:
    expectedException.expect(KsqlServerException.class);
    expectedException.expectCause(is(instanceOf(KsqlTopicAuthorizationException.class)));

    // When:
    distributor
        .execute(configuredStatement, ImmutableMap.of(), executionContext, userServiceContext);
  }
}
