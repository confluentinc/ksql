/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.server.computation.CommandId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressFBWarnings("NP_NULL_PARAM_DEREF_NONVIRTUAL")
public class CommandStatusEntityTest {
  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  private static final String JSON_ENTITY = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\""
      + "},"
      + "\"commandSequenceNumber\":2,"
      + "\"warnings\":[]"
      + "}";
  private static final String JSON_ENTITY_NO_WARNINGS = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\""
      + "},"
      + "\"commandSequenceNumber\":2"
      + "}";
  private static final String JSON_ENTITY_NO_CSN = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\""
      + "}}";

  private static final String STATEMENT_TEXT = "sql";
  private static final CommandId COMMAND_ID = CommandId.fromString("topic/1/create");
  private static final CommandStatus COMMAND_STATUS =
      new CommandStatus(CommandStatus.Status.SUCCESS, "some success message");
  private static final long COMMAND_SEQUENCE_NUMBER = 2L;
  private static final CommandStatusEntity ENTITY =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS, COMMAND_SEQUENCE_NUMBER);
  private static final CommandStatusEntity ENTITY_WITHOUT_SEQUENCE_NUMBER =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS, null);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldSerializeToJson() throws Exception {
    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(ENTITY);

    // Then:
    assertThat(json, is(JSON_ENTITY));
  }

  @Test
  public void shouldDeserializeFromJson() throws Exception {
    // When:
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY, CommandStatusEntity.class);

    // Then:
    assertThat(entity, is(ENTITY));
  }

  @Test
  public void shouldBeAbleToDeserializeOlderServerMessage() throws Exception {
    // When:
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY_NO_CSN, CommandStatusEntity.class);

    // Then:
    assertThat(entity, is(ENTITY_WITHOUT_SEQUENCE_NUMBER));
  }

  @Test
  public void shouldHandleNullSequenceNumber() {
    // When:
    final CommandStatusEntity entity =
        new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS, null);

    // Then:
    assertThat(entity.getCommandSequenceNumber(), is(-1L));
  }

  @Test
  public void shouldThrowOnNullCommandId() {
    // Given:
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("commandId");

    // When:
    new CommandStatusEntity(STATEMENT_TEXT, null, COMMAND_STATUS, COMMAND_SEQUENCE_NUMBER);
  }

  @Test
  public void shouldThrowOnNullCommandStatus() {
    // Given:
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("commandStatus");

    // When:
    new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, null, COMMAND_SEQUENCE_NUMBER);
  }
}