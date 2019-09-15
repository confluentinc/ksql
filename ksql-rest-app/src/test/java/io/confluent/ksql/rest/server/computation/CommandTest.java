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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class CommandTest {
  @Test
  public void shouldDeserializeCorrectly() throws IOException {
    final String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"useOffsetAsQueryID\": true, " +
        "\"streamsProperties\": {\"foo\": \"bar\"}, " +
        "\"originalProperties\": {\"biz\": \"baz\"} " +
        "}";
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final Command command = mapper.readValue(commandStr, Command.class);
    assertThat(command.getStatement(), equalTo("test statement;"));
    final Map<String, Object> expecteOverwriteProperties
        = Collections.singletonMap("foo", "bar");
    assertThat(command.getOverwriteProperties(), equalTo(expecteOverwriteProperties));
    final Map<String, Object> expectedOriginalProperties
        = Collections.singletonMap("biz", "baz");
    assertThat(command.getOriginalProperties(), equalTo(expectedOriginalProperties));
    assertThat(command.isPreVersion5(), is(false));
    assertThat(command.getUseOffsetAsQueryID(), is(true));
  }

  @Test
  public void shouldDeserializeWithoutKsqlConfigCorrectly() throws IOException {
    final String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"useOffsetAsQueryID\": true, " +
        "\"streamsProperties\": {\"foo\": \"bar\"}" +
        "}";
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final Command command = mapper.readValue(commandStr, Command.class);
    assertThat(command.getStatement(), equalTo("test statement;"));
    final Map<String, Object> expecteOverwriteProperties = Collections.singletonMap("foo", "bar");
    assertThat(command.getOverwriteProperties(), equalTo(expecteOverwriteProperties));
    assertThat(command.getOriginalProperties(), equalTo(Collections.emptyMap()));
    assertThat(command.isPreVersion5(), equalTo(true));
    assertThat(command.getUseOffsetAsQueryID(), is(true));
  }

  @Test
  public void shouldDeserializeWithoutUseOffsetAsQueryIDCorrectly() throws IOException {
    final String commandStr = "{" +
            "\"statement\": \"test statement;\", " +
            "\"streamsProperties\": {\"foo\": \"bar\"}, " +
            "\"originalProperties\": {\"biz\": \"baz\"} " +
            "}";
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final Command command = mapper.readValue(commandStr, Command.class);
    assertThat(command.getStatement(), equalTo("test statement;"));
    final Map<String, Object> expecteOverwriteProperties
            = Collections.singletonMap("foo", "bar");
    assertThat(command.getOverwriteProperties(), equalTo(expecteOverwriteProperties));
    final Map<String, Object> expectedOriginalProperties
            = Collections.singletonMap("biz", "baz");
    assertThat(command.getOriginalProperties(), equalTo(expectedOriginalProperties));
    assertThat(command.isPreVersion5(), is(false));
    assertThat(command.getUseOffsetAsQueryID(), is(false));
  }

  void grep(final String string, final String regex) {
    assertThat(string.matches(regex), is(true));
  }

  @Test
  public void shouldSerializeDeserializeCorrectly() throws IOException {
    final Command command = new Command(
        "test statement;", true,
            Collections.singletonMap("foo", "bar"), Collections.singletonMap("biz", "baz"));
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final String serialized = mapper.writeValueAsString(command);
    grep(serialized, ".*\"streamsProperties\" *: *\\{ *\"foo\" *: *\"bar\" *\\}.*");
    grep(serialized, ".*\"statement\" *: *\"test statement;\".*");
    grep(serialized, ".*\"useOffsetAsQueryID\".*:true.*");
    grep(serialized, ".*\"originalProperties\" *: *\\{ *\"biz\" *: *\"baz\" *\\}.*");
    final Command deserialized = mapper.readValue(serialized, Command.class);
    assertThat(deserialized, equalTo(command));
  }
}
