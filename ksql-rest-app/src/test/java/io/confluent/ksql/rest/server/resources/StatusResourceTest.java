/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.junit.Test;

public class StatusResourceTest {

  private static final Map<CommandId, CommandStatus> mockCommandStatuses;

  static {
    mockCommandStatuses = new HashMap<>();

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.TOPIC, "test_topic", CommandId.Action.CREATE),
        new CommandStatus(CommandStatus.Status.SUCCESS, "Topic created successfully")
    );

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.STREAM, "test_stream", CommandId.Action.CREATE),
        new CommandStatus(CommandStatus.Status.ERROR, "Hi Ewen!")
    );

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.TERMINATE, "5", CommandId.Action.CREATE),
        new CommandStatus(CommandStatus.Status.QUEUED, "Command written to command topic")
    );
  }

  private StatusResource getTestStatusResource() {
    final StatementExecutor mockStatementExecutor = mock(StatementExecutor.class);

    expect(mockStatementExecutor.getStatuses()).andReturn(mockCommandStatuses);

    for (final Map.Entry<CommandId, CommandStatus> commandEntry : mockCommandStatuses.entrySet()) {
      expect(mockStatementExecutor.getStatus(commandEntry.getKey())).andReturn(Optional.of(commandEntry.getValue()));
    }

    expect(mockStatementExecutor.getStatus(anyObject(CommandId.class))).andReturn(Optional.empty());

    replay(mockStatementExecutor);

    return new StatusResource(mockStatementExecutor);
  }

  @Test
  public void testGetAllStatuses() {
    final StatusResource testResource = getTestStatusResource();

    final Object statusesEntity = testResource.getAllStatuses().getEntity();
    assertThat(statusesEntity, instanceOf(CommandStatuses.class));
    final CommandStatuses testCommandStatuses = (CommandStatuses) statusesEntity;

    final Map<CommandId, CommandStatus.Status> expectedCommandStatuses =
        CommandStatuses.fromFullStatuses(mockCommandStatuses);

    assertEquals(expectedCommandStatuses, testCommandStatuses);
  }

  @Test
  public void testGetStatus() throws Exception {
    final StatusResource testResource = getTestStatusResource();

    for (final Map.Entry<CommandId, CommandStatus> commandEntry : mockCommandStatuses.entrySet()) {
      final CommandId commandId = commandEntry.getKey();
      final CommandStatus expectedCommandStatus = commandEntry.getValue();

      final Object statusEntity = testResource.getStatus(commandId.getType().name(), commandId.getEntity(), commandId.getAction().name()).getEntity();
      assertThat(statusEntity, instanceOf(CommandStatus.class));
      final CommandStatus testCommandStatus = (CommandStatus) statusEntity;

      assertEquals(expectedCommandStatus, testCommandStatus);
    }
  }

  @Test
  public void testGetStatusNotFound() throws Exception {
    final StatusResource testResource = getTestStatusResource();
    final Response response = testResource.getStatus(
        CommandId.Type.STREAM.name(), "foo", CommandId.Action.CREATE.name());
    assertThat(response.getStatus(), equalTo(Response.Status.NOT_FOUND.getStatusCode()));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_NOT_FOUND));
    assertThat(errorMessage.getMessage(), equalTo("Command not found"));
  }
}
