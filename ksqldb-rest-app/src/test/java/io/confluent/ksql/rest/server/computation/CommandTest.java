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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.json.PlanJsonMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.rest.server.resources.IncompatibleKsqlCommandVersionException;
import org.hamcrest.Matchers;
import org.junit.Test;

public class CommandTest {

  @Test
  public void shouldDeserializeCorrectly() throws IOException {
    final String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"streamsProperties\": {\"foo\": \"bar\"}, " +
        "\"originalProperties\": {\"biz\": \"baz\"} " +
        "}";
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
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
  public void shouldThrowExceptionWhenCommandVersionHigher() {
    final String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"streamsProperties\": {\"foo\": \"bar\"}, " +
        "\"originalProperties\": {\"biz\": \"baz\"}, " +
        "\"version\": " + (Command.VERSION + 1) +
        "}";
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final ValueInstantiationException thrown = assertThrows(
        "Expected deserialization to throw, but it didn't",
        ValueInstantiationException.class,
        () -> mapper.readValue(commandStr, Command.class)
    );
    assertTrue(thrown.getCause() instanceof IncompatibleKsqlCommandVersionException);
  }

  @Test
  public void shouldDeserializeCorrectlyWithVersion() throws IOException {
    final String commandStr = "{" +
        "\"statement\": \"test statement;\", " +
        "\"streamsProperties\": {\"foo\": \"bar\"}, " +
        "\"originalProperties\": {\"biz\": \"baz\"}, " +
        "\"version\": " + Command.VERSION +
        "}";
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final Command command = mapper.readValue(commandStr, Command.class);
    assertThat(command.getStatement(), equalTo("test statement;"));
    final Map<String, Object> expecteOverwriteProperties
        = Collections.singletonMap("foo", "bar");
    assertThat(command.getOverwriteProperties(), equalTo(expecteOverwriteProperties));
    final Map<String, Object> expectedOriginalProperties
        = Collections.singletonMap("biz", "baz");
    assertThat(command.getOriginalProperties(), equalTo(expectedOriginalProperties));
    assertThat(command.getVersion(), is(Optional.of(Command.VERSION)));
  }

  private void grep(final String string, final String regex) {
    assertThat(String.format("[%s] does not match [%s]", string, regex), string.matches(regex), is(true));

  }

  @Test
  public void shouldSerializeDeserializeCorrectly() throws IOException {
    final Command command = new Command(
        "test statement;",
        Collections.singletonMap("foo", "bar"), Collections.singletonMap("biz", "baz"),
        Optional.empty());
    final ObjectMapper mapper = PlanJsonMapper.INSTANCE.get();
    final String serialized = mapper.writeValueAsString(command);
    grep(serialized, ".*\"streamsProperties\" *: *\\{ *\"foo\" *: *\"bar\" *\\}.*");
    grep(serialized, ".*\"statement\" *: *\"test statement;\".*");
    grep(serialized, ".*\"originalProperties\" *: *\\{ *\"biz\" *: *\"baz\" *\\}.*");
    grep(serialized, ".*\"version\" *: *" + Command.VERSION + ".*");
    final Command deserialized = mapper.readValue(serialized, Command.class);
    assertThat(deserialized, equalTo(command));
  }

  @Test
  public void shouldCoerceProperties() {
    // Given/When:
    final Command command = new Command(
        "test statement;",
        ImmutableMap.of(
            "ksql.internal.topic.replicas", 3L
        ),
        Collections.emptyMap(),
        Optional.empty()
    );

    // Then:
    assertThat(
        command.getOverwriteProperties().get("ksql.internal.topic.replicas"),
        instanceOf(Short.class)
    );
    assertThat(
        command.getOverwriteProperties().get("ksql.internal.topic.replicas"),
        Matchers.equalTo((short) 3)
    );
  }

  @Test
  public void shouldMigrateLegacyExactlyOnceFromCommandTopic() {
    // Given: Command from old command topic with legacy 'exactly_once' value
    final Command command = new Command(
        "CREATE STREAM test AS SELECT * FROM source;",
        ImmutableMap.of(
            "processing.guarantee", "exactly_once",
            "num.stream.threads", 4
        ),
        Collections.emptyMap(),
        Optional.empty()
    );

    // When: Getting overwrite properties (triggers migration)
    final Map<String, Object> properties = command.getOverwriteProperties();

    // Then: Should be migrated to exactly_once_v2
    assertThat(properties.get("processing.guarantee"), equalTo("exactly_once_v2"));
    // Other properties should remain unchanged
    assertThat(properties.get("num.stream.threads"), equalTo(4));
  }

  @Test
  public void shouldMigrateLegacyAtMostOnceFromCommandTopic() {
    // Given: Command from old command topic with legacy 'at_most_once' value
    final Command command = new Command(
        "CREATE STREAM test AS SELECT * FROM source;",
        ImmutableMap.of(
            "processing.guarantee", "at_most_once"
        ),
        Collections.emptyMap(),
        Optional.empty()
    );

    // When: Getting overwrite properties (triggers migration)
    final Map<String, Object> properties = command.getOverwriteProperties();

    // Then: Should be migrated to at_least_once
    assertThat(properties.get("processing.guarantee"), equalTo("at_least_once"));
  }

  @Test
  public void shouldNotMigrateModernProcessingGuaranteeValues() {
    // Given: Command with modern processing guarantee values
    final Command command1 = new Command(
        "CREATE STREAM test AS SELECT * FROM source;",
        ImmutableMap.of("processing.guarantee", "exactly_once_v2"),
        Collections.emptyMap(),
        Optional.empty()
    );
    final Command command2 = new Command(
        "CREATE STREAM test AS SELECT * FROM source;",
        ImmutableMap.of("processing.guarantee", "at_least_once"),
        Collections.emptyMap(),
        Optional.empty()
    );

    // When: Getting overwrite properties
    final Map<String, Object> properties1 = command1.getOverwriteProperties();
    final Map<String, Object> properties2 = command2.getOverwriteProperties();

    // Then: Should remain unchanged
    assertThat(properties1.get("processing.guarantee"), equalTo("exactly_once_v2"));
    assertThat(properties2.get("processing.guarantee"), equalTo("at_least_once"));
  }

  @Test
  public void shouldHandleCommandWithNoProcessingGuarantee() {
    // Given: Command without processing.guarantee property
    final Command command = new Command(
        "CREATE STREAM test AS SELECT * FROM source;",
        ImmutableMap.of("num.stream.threads", 4),
        Collections.emptyMap(),
        Optional.empty()
    );

    // When: Getting overwrite properties
    final Map<String, Object> properties = command.getOverwriteProperties();

    // Then: Should not add processing.guarantee
    assertThat(properties.containsKey("processing.guarantee"), is(false));
    assertThat(properties.get("num.stream.threads"), equalTo(4));
  }
}
