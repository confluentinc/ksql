/*
 * Copyright 2019 Confluent Inc.
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
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import java.util.Optional;
import org.junit.Test;

@SuppressFBWarnings("NP_NULL_PARAM_DEREF_NONVIRTUAL")
public class CommandStatusEntityTest {
  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private static final String JSON_ENTITY = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\","
      + "\"queryId\":\"CSAS_0\""
      + "},"
      + "\"commandSequenceNumber\":2,"
      + "\"warnings\":[]"
      + "}";
  private static final String JSON_ENTITY_EMPTY_QUERY_ID = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\","
      + "\"queryId\":null"
      + "},"
      + "\"commandSequenceNumber\":2,"
      + "\"warnings\":[]"
      + "}";
  private static final String JSON_ENTITY_NO_QUERY_ID = "{"
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
  private static final String JSON_ENTITY_NO_CSN = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\""
      + "}}";
  private static final String JSON_ENTITY_WITH_WARNINGS = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\","
      + "\"queryId\":\"CSAS_0\""
      + "},"
      + "\"commandSequenceNumber\":2,"
      + "\"warnings\":[{\"message\":\"Warning 1\"},{\"message\":\"Warning 2\"}]"
      + "}";

  private static final String STATEMENT_TEXT = "sql";
  private static final CommandId COMMAND_ID = CommandId.fromString("topic/1/create");
  private static final CommandStatus COMMAND_STATUS =
      new CommandStatus(
          CommandStatus.Status.SUCCESS,
          "some success message",
          Optional.of(new QueryId("CSAS_0"))
      );
  private static final CommandStatus COMMAND_STATUS_WITHOUT_QUERY_ID =
      new CommandStatus(CommandStatus.Status.SUCCESS, "some success message");
  private static final long COMMAND_SEQUENCE_NUMBER = 2L;
  private static final CommandStatusEntity ENTITY =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS, COMMAND_SEQUENCE_NUMBER);
  private static final CommandStatusEntity ENTITY_WITHOUT_QUERY_ID =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS_WITHOUT_QUERY_ID, COMMAND_SEQUENCE_NUMBER);
  private static final CommandStatusEntity ENTITY_WITHOUT_SEQUENCE_NUMBER =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS_WITHOUT_QUERY_ID, null);
  private static final CommandStatusEntity ENTITY_WITH_WARNINGS =
      new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, COMMAND_STATUS, COMMAND_SEQUENCE_NUMBER,
          ImmutableList.of(
              new KsqlWarning("Warning 1"),
              new KsqlWarning("Warning 2")
          ));

  @Test
  public void shouldSerializeToJson() throws Exception {
    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(ENTITY);

    // Then:
    assertThat(json, is(JSON_ENTITY));
  }

  @Test
  public void shouldSerializeToJsonWithEmptyQueryId() throws Exception {
    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(ENTITY_WITHOUT_QUERY_ID);

    // Then:
    assertThat(json, is(JSON_ENTITY_EMPTY_QUERY_ID));
  }

  @Test
  public void shouldSerializeToJsonWithWarnings() throws Exception {
    // when:
    final String json = OBJECT_MAPPER.writeValueAsString(ENTITY_WITH_WARNINGS);

    // then:
    assertThat(json, is(JSON_ENTITY_WITH_WARNINGS));
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
  public void shouldDeserializeFromJsonWithEmptyQueryId() throws Exception {
    // When:
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY_EMPTY_QUERY_ID, CommandStatusEntity.class);

    // Then:
    assertThat(entity, is(ENTITY_WITHOUT_QUERY_ID));
  }

  @Test
  public void shouldDeserializeFromJsonWithWarnings() throws Exception {
    // When:
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY_WITH_WARNINGS, CommandStatusEntity.class);

    // Then:
    assertThat(entity, is(ENTITY_WITH_WARNINGS));
  }

  @Test
  public void shouldBeAbleToDeserializeOlderServerMessageWithNoQueryId() throws Exception {
    // When:
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY_NO_QUERY_ID, CommandStatusEntity.class);

    // Then:
    assertThat(entity, is(ENTITY_WITHOUT_QUERY_ID));
  }

  @Test
  public void shouldBeAbleToDeserializeOlderServerMessageWithNoCSN() throws Exception {
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
    // When:
    final Exception e = assertThrows(
        NullPointerException.class,
        () -> new CommandStatusEntity(STATEMENT_TEXT, null, COMMAND_STATUS, COMMAND_SEQUENCE_NUMBER)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "commandId"));
  }

  @Test
  public void shouldThrowOnNullCommandStatus() {
    // When:
    final Exception e = assertThrows(
        NullPointerException.class,
        () -> new CommandStatusEntity(STATEMENT_TEXT, COMMAND_ID, null, COMMAND_SEQUENCE_NUMBER)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "commandStatus"));
  }
}