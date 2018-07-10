/**
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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.util.JsonMapper;
import org.junit.Assert;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class CommandTest {
  @Test
  public void shouldDeserializeCorrectly() throws IOException {
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
  }

  @Test
  public void shouldDeserializeWithoutKsqlConfigCorrectly() throws IOException {
    String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"streamsProperties\": {\"foo\": \"bar\"}" +
        "}";
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    Command command = mapper.readValue(commandStr, Command.class);
    assertThat(command.getStatement(), equalTo("test statement;"));
    Map<String, Object> expecteOverwriteProperties = Collections.singletonMap("foo", "bar");
    assertThat(command.getOverwriteProperties(), equalTo(expecteOverwriteProperties));
    assertThat(command.getOriginalProperties(), equalTo(Collections.emptyMap()));
  }

  void grep(String string, String regex) {
    assertThat(string.matches(regex), is(true));
  }

  @Test
  public void shouldSerializeDeserializeCorrectly() throws IOException {
    Command command = new Command(
        "test statement;",
        Collections.singletonMap("foo", "bar"),
        Collections.singletonMap("biz", "baz"));
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    String serialized = mapper.writeValueAsString(command);
    grep(serialized, ".*\"streamsProperties\" *: *\\{ *\"foo\" *: *\"bar\" *\\}.*");
    grep(serialized, ".*\"statement\" *: *\"test statement;\".*");
    grep(serialized, ".*\"originalProperties\" *: *\\{ *\"biz\" *: *\"baz\" *\\}.*");
    Command deserialized = mapper.readValue(serialized, Command.class);
    assertThat(deserialized, equalTo(command));
  }
}
