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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.EmptyStatement;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
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

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock CommandQueue queue;
  @Mock QueuedCommandStatus status;
  @Mock ServiceContext serviceContext;
  @Mock SchemaInjector schemaInjector;

  @Before
  public void setUp() {
    when(schemaInjector.forStatement(any())).thenAnswer(inv -> inv.getArgument(0));
  }


  @Test
  public void shouldEnqueueSuccessfulCommand() throws InterruptedException {
    // Given:
    final CommandId command = new CommandId(Type.STREAM, "stream", Action.CREATE);
    final CommandStatus commandStatus = new CommandStatus(Status.SUCCESS, "");

    when(queue.enqueueCommand(any(), any(), any())).thenReturn(status);
    when(status.tryWaitForFinalStatus(any())).thenReturn(commandStatus);
    when(status.getCommandId()).thenReturn(command);
    when(status.getCommandSequenceNumber()).thenReturn(1L);

    final PreparedStatement<?> preparedStatement = PreparedStatement.of("", new EmptyStatement());

    // When:
    final CommandStatusEntity commandStatusEntity =
        (CommandStatusEntity) new DistributingExecutor(queue, DURATION_10_MS, sc -> schemaInjector)
            .execute(preparedStatement, null, serviceContext, null, null)
            .orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(commandStatusEntity, equalTo(new CommandStatusEntity(
        "",
        command,
        commandStatus,
        1L
    )));

    verify(queue, times(1)).enqueueCommand(eq(preparedStatement), any(), any());
    verify(schemaInjector, times(1)).forStatement(eq(preparedStatement));
  }

  @Test
  public void testFailingCommandQueue() {
    // Given:
    when(queue.enqueueCommand(any(), any(), any())).thenThrow(new KsqlException("Fail!"));
    final PreparedStatement preparedStatement = PreparedStatement.of("", new EmptyStatement());

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not write the statement");

    // When:
    new DistributingExecutor(queue, DURATION_10_MS, context -> schemaInjector)
        .execute(preparedStatement, null, serviceContext, null, null);
  }

  @Test
  public void testCannotInferSchema() {
    // Given:
    final PreparedStatement preparedStatement = PreparedStatement.of("", new EmptyStatement());
    when(schemaInjector.forStatement(any())).thenThrow(new KsqlException("Could not infer!"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not infer!");

    // When:
    new DistributingExecutor(queue, DURATION_10_MS, context -> schemaInjector)
        .execute(preparedStatement, null, serviceContext, null, null);
  }

}
