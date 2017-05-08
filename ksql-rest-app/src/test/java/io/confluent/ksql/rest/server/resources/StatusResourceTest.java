package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStatus;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StatusResourceTest {

  private static final Map<CommandId, CommandStatus> mockCommandStatuses;

  static {
    mockCommandStatuses = new HashMap<>();

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.TOPIC, "test_topic"),
        new CommandStatus(CommandStatus.Status.SUCCESS, "Topic created successfully")
    );

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.STREAM, "test_stream"),
        new CommandStatus(CommandStatus.Status.ERROR, "Hi Ewen!")
    );

    mockCommandStatuses.put(
        new CommandId(CommandId.Type.TERMINATE, "5"),
        new CommandStatus(CommandStatus.Status.QUEUED, "Command written to command topic")
    );
  }

  private StatusResource getTestStatusResource() {
    StatementExecutor mockStatementExecutor = mock(StatementExecutor.class);

    expect(mockStatementExecutor.getStatuses()).andReturn(mockCommandStatuses);

    for (Map.Entry<CommandId, CommandStatus> commandEntry : mockCommandStatuses.entrySet()) {
      expect(mockStatementExecutor.getStatus(commandEntry.getKey())).andReturn(Optional.of(commandEntry.getValue()));
    }

    expect(mockStatementExecutor.getStatus(anyObject(CommandId.class))).andReturn(Optional.empty());

    replay(mockStatementExecutor);

    return new StatusResource(mockStatementExecutor);
  }

  private JsonStructure parseResponse(Response response) {
    return Json.createReader(new ByteArrayInputStream(((String) response.getEntity()).getBytes())).read();
  }

  private JsonValue getObjectField(JsonObject object, String field) {
    assertTrue(object.containsKey(field));
    return object.get(field);
  }

  private <T extends JsonValue>T getElementAs(JsonValue element, Class<T> klass) {
    assertThat(element, instanceOf(klass));
    return klass.cast(element);
  }

  private <T extends JsonValue>T getObjectFieldAs(JsonObject object, String field, Class<T> klass) {
    return getElementAs(getObjectField(object, field), klass);
  }

  private String getObjectFieldAsString(JsonObject object, String field) {
    return getObjectFieldAs(object, field, JsonString.class).getString();
  }

  @Test
  public void testGetAllStatuses() {
    StatusResource testResource = getTestStatusResource();

    JsonStructure responseStructure = parseResponse(testResource.getAllStatuses());
    JsonObject responseObject = getElementAs(responseStructure, JsonObject.class);
    JsonObject statusesObject = getObjectFieldAs(responseObject, "statuses", JsonObject.class);

    assertEquals(mockCommandStatuses.size(), statusesObject.size());
    for (Map.Entry<CommandId, CommandStatus> commandEntry : mockCommandStatuses.entrySet()) {
      String commandId = commandEntry.getKey().toString();
      String expectedStatus = commandEntry.getValue().getStatus().toString();
      String testStatus = getObjectFieldAsString(statusesObject, commandId);
      assertEquals(expectedStatus, testStatus);
    }
  }

  @Test
  public void testGetStatus() throws Exception {
    StatusResource testResource = getTestStatusResource();

    for (Map.Entry<CommandId, CommandStatus> commandEntry : mockCommandStatuses.entrySet()) {
      CommandId commandId = commandEntry.getKey();
      CommandStatus commandStatus = commandEntry.getValue();

      JsonStructure responseStructure = parseResponse(
          testResource.getStatus(commandId.getType().toString(), commandId.getEntity())
      );

      JsonObject responseObject = getElementAs(responseStructure, JsonObject.class);
      JsonObject statusObject = getObjectFieldAs(responseObject, "status", JsonObject.class);

      String testCommandId = getObjectFieldAsString(statusObject, "command_id");
      assertEquals(commandId.toString(), testCommandId);

      String testStatus = getObjectFieldAsString(statusObject, "status");
      assertEquals(commandStatus.getStatus().toString(), testStatus);

      String testMessage = getObjectFieldAsString(statusObject, "message");
      assertEquals(commandStatus.getMessage(), testMessage);
    }
  }
}
