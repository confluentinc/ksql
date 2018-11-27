/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.server.computation.CommandId;
import org.junit.Test;

public class CommandStatusEntityTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String JSON_ENTITY = "{"
      + "\"@type\":\"currentStatus\","
      + "\"statementText\":\"sql\","
      + "\"commandId\":\"topic/1/create\","
      + "\"commandStatus\":{"
      + "\"status\":\"SUCCESS\","
      + "\"message\":\"some success message\""
      + "},"
      + "\"commandOffset\":0"
      + "}";

  private static final CommandStatusEntity ENTITY = new CommandStatusEntity(
      "sql",
      CommandId.fromString("topic/1/create"),
      new CommandStatus(CommandStatus.Status.SUCCESS, "some success message"),
      0);

  @Test
  public void shouldSerializeToJson() throws Exception {
    final String json = OBJECT_MAPPER.writeValueAsString(ENTITY);
    assertThat(json, is(JSON_ENTITY));
  }

  @Test
  public void shouldDeserializeFromJson() throws Exception {
    final CommandStatusEntity entity =
        OBJECT_MAPPER.readValue(JSON_ENTITY, CommandStatusEntity.class);
    assertThat(entity, is(ENTITY));
  }
}