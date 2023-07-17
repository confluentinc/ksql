/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericDeserializerTest {

  private static final Map<String, ?> SOME_CONFIG = ImmutableMap.of("some", "thing");
  private static final byte[] SERIALIZED = "serialized".getBytes(StandardCharsets.UTF_8);

  @Mock
  private Deserializer<List<?>> innerDeserializer;
  private GenericDeserializer<TestListWrapper> deserializer;

  @Before
  public void setUp() {
    deserializer = new GenericDeserializer<>(TestListWrapper::new, innerDeserializer, 2);
  }

  @Test
  public void shouldConfigureInnerDeserializerOnConfigure() {
    // When:
    deserializer.configure(SOME_CONFIG, true);

    // Then:
    verify(innerDeserializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldCloseInnerDeserializerOnClose() {
    // When:
    deserializer.close();

    // Then:
    verify(innerDeserializer).close();
  }

  @Test
  public void shouldDeserializeNulls() {
    // Given:
    when(innerDeserializer.deserialize(any(), any())).thenReturn(null);

    // When:
    final TestListWrapper result = deserializer.deserialize("topic", SERIALIZED);

    // Then:
    verify(innerDeserializer).deserialize("topic", SERIALIZED);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldThrowOnDeserializeOnColumnCountMismatch() {
    // Given:
    givenInnerDeserializerReturns(ImmutableList.of("too", "many", "columns"));

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("topicName", SERIALIZED)
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on deserialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldConvertListToRowWhenDeserializing() {
    // Given:
    givenInnerDeserializerReturns(ImmutableList.of("world", -10));

    // When:
    final TestListWrapper result = deserializer.deserialize("topicName", SERIALIZED);

    // Then:
    assertThat(result.getList(), is(ImmutableList.of("world", -10)));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void givenInnerDeserializerReturns(final List<?> values) {
    when(innerDeserializer.deserialize(any(), any())).thenReturn((List)values);
  }

  public static final class TestListWrapper {

    private final List<?> values;

    public TestListWrapper(final List<?> values) {
      this.values = values;
    }

    public List<?> getList() {
      return values;
    }
  }
}