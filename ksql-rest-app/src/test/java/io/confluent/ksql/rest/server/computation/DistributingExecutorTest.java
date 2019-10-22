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
import io.confluent.ksql.rest.server.ProducerTransactionManager;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.ServiceContext;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DistributingExecutorTest {

  private static final Duration DURATION_10_MS = Duration.ofMillis(10);
  private static final CommandId CS_COMMAND = new CommandId(Type.STREAM, "stream", Action.CREATE);
  private static final CommandStatus SUCCESS_STATUS = new CommandStatus(Status.SUCCESS, "");
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(new HashMap<>());
  private static final ConfiguredStatement<Statement> EMPTY_STATEMENT =
      ConfiguredStatement.of(
          PreparedStatement.of("", new ListProperties(Optional.empty())),
          ImmutableMap.of(),
          KSQL_CONFIG
      );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock CommandQueue queue;
  @Mock QueuedCommandStatus status;
  @Mock ServiceContext serviceContext;
  @Mock Injector schemaInjector;
  @Mock Injector topicInjector;
  @Mock KsqlAuthorizationValidator authorizationValidator;
  @Mock KsqlExecutionContext executionContext;
  @Mock MetaStore metaStore;
  @Mock ProducerTransactionManager producerTransactionManager;

  private DistributingExecutor distributor;
  private AtomicLong scnCounter;

  @Before
  public void setUp() throws InterruptedException {
    scnCounter = new AtomicLong();
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(queue.enqueueCommand(any(), any(ProducerTransactionManager.class))).thenReturn(status);
    when(status.tryWaitForFinalStatus(any())).thenReturn(SUCCESS_STATUS);
    when(status.getCommandId()).thenReturn(CS_COMMAND);
    when(status.getCommandSequenceNumber()).thenAnswer(inv -> scnCounter.incrementAndGet());
    when(executionContext.getMetaStore()).thenReturn(metaStore);
    when(executionContext.getServiceContext()).thenReturn(serviceContext);

    distributor = new DistributingExecutor(
        queue,
        DURATION_10_MS,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        authorizationValidator
    );
    distributor.setTransactionManager(producerTransactionManager);
  }

  @Test
  public void shouldEnqueueSuccessfulCommand() throws InterruptedException {
    // When:
    distributor.execute(EMPTY_STATEMENT, ImmutableMap.of(), executionContext, serviceContext);

    // Then:
    verify(queue, times(1)).enqueueCommand(eq(EMPTY_STATEMENT), any());
  }

  @Test
  public void shouldInferSchemas() {
    // When:
    distributor.execute(EMPTY_STATEMENT, ImmutableMap.of(), executionContext, serviceContext);

    // Then:
    verify(schemaInjector, times(1)).inject(eq(EMPTY_STATEMENT));
  }

  @Test
  public void shouldReturnCommandStatus() {
    // When:
    final CommandStatusEntity commandStatusEntity =
        (CommandStatusEntity) distributor.execute(
            EMPTY_STATEMENT,
            ImmutableMap.of(),
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
    when(queue.enqueueCommand(any(), any(ProducerTransactionManager.class))).thenThrow(cause);

    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of("x", new ListProperties(Optional.empty()));

    final ConfiguredStatement<Statement> configured =
        ConfiguredStatement.of(
            preparedStatement,
            ImmutableMap.of(),
            KSQL_CONFIG);

    // Expect:
    expectedException.expect(KsqlServerException.class);
    expectedException.expectMessage(
        "Could not write the statement 'x' into the command topic: fail");
    expectedException.expectCause(is(cause));

    // When:
    distributor.execute(configured, ImmutableMap.of(), executionContext, serviceContext);
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
    distributor.execute(configured, ImmutableMap.of(), executionContext, serviceContext);
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
    distributor.execute(configured, ImmutableMap.of(), executionContext, userServiceContext);
  }

  @Test
  public void shouldThrowServerExceptionIfServerServiceContextIsDeniedAuthorization() {
    // Given:
    final ServiceContext userServiceContext = mock(ServiceContext.class);
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
    distributor.execute(configured, ImmutableMap.of(), executionContext, userServiceContext);
  }
}
