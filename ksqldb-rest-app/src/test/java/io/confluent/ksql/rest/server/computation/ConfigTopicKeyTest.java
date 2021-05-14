/*
 * Copyright 2021 Confluent Inc.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.rest.server.computation.ConfigTopicKey.StringKey;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class ConfigTopicKeyTest {
  private static final StringKey STRING_KEY = new StringKey("string-key-value");
  private final byte[] SERIALIZED
      = "{\"string\":{\"value\":\"string-key-value\"}}".getBytes(StandardCharsets.UTF_8);

  private final Serializer<ConfigTopicKey> serializer = InternalTopicSerdes.serializer();
  private final Deserializer<ConfigTopicKey> deserializer
      = InternalTopicSerdes.deserializer(ConfigTopicKey.class);

  @Test
  public void shouldImplementEqualsForStringKey() {
    new EqualsTester()
        .addEqualityGroup(new StringKey("foo"), new StringKey("foo"))
        .addEqualityGroup(new StringKey("bar"))
        .testEquals();
  }

  @Test
  public void shouldSerializeStringKey() {
    // When:
    final byte[] bytes = serializer.serialize("", STRING_KEY);

    // Then:
    assertThat(bytes, equalTo(SERIALIZED));
  }

  @Test
  public void shouldDeserializeStringKey() {
    // When:
    final ConfigTopicKey key = deserializer.deserialize("", SERIALIZED);

    // Then:
    assertThat(key, equalTo(STRING_KEY));
  }

  private static class IllegalArgumentMatcher extends TypeSafeMatcher<Exception> {
    private final String msg;
    private final Class<? extends Exception> exceptionClass;

    IllegalArgumentMatcher(final Class<? extends Exception> exceptionClass, final String msg) {
      this.msg = msg;
      this.exceptionClass = exceptionClass;
    }

    @Override
    public boolean matchesSafely(final Exception e) {
      return e instanceof SerializationException
          && e.getCause() instanceof InvalidDefinitionException || e.getCause() instanceof ValueInstantiationException
          && exceptionClass.isInstance(e.getCause().getCause())
          && e.getCause().getCause().getMessage().contains(msg);
    }

    @Override
    public void describeTo(final Description description) {
      description.appendValue(
          "SerializationException w/ cause IllegalArgumentException(" + msg + ")");
    }
  }

  private IllegalArgumentMatcher illegalString(
      final Class<? extends Exception> exceptionClass,
      final String msg) {
    return new IllegalArgumentMatcher(exceptionClass, msg);
  }

  @Test
  public void shouldThrowOnStringKeyWithNoValue() {
    // When:
    assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("", "{\"string\":{}}".getBytes(UTF_8))
    );
  }

  @Test
  public void shouldThrowOnStringKeyWithEmptyValue() {
    // When:
    final SerializationException e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("", "{\"string\":{\"value\": \"\"}}".getBytes(StandardCharsets.UTF_8))
    );

    // Then:
    assertThat(e, illegalString(IllegalArgumentException.class, "StringKey value must not be empty"));
  }
}