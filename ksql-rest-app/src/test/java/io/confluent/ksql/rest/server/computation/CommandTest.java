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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class CommandTest {
  @Test
  public void shouldDeserializeCorrectly() throws IOException {
    String commandStr = "{\"statement\": \"test statement;\", \"streamsProperties\": {\"foo\": \"bar\"}}";
    ObjectMapper mapper = new ObjectMapper();
    Command command = mapper.readValue(commandStr, Command.class);
    Assert.assertThat(command.getStatement(), equalTo("test statement;"));
    Map<String, Object> expectedKsqlProperties = Collections.singletonMap("foo", "bar");
    Assert.assertThat(command.getKsqlProperties(), equalTo(expectedKsqlProperties));
  }

  void grep(String string, String regex) {
    Assert.assertThat(string.matches(regex), is(true));
  }

  @Test
  public void shouldSerializeDeserializeCorrectly() throws IOException {
    Command command = new Command("test statement;", Collections.singletonMap("foo", "bar"));
    ObjectMapper mapper = new ObjectMapper();
    String serialized = mapper.writeValueAsString(command);
    grep(serialized, ".*\"streamsProperties\" *: *\\{ *\"foo\" *: *\"bar\" *\\}.*");
    grep(serialized, ".*\"statement\" *: *\"test statement;\".*");
    Command deserialized = mapper.readValue(serialized, Command.class);
    Assert.assertThat(deserialized, equalTo(command));
  }
}
