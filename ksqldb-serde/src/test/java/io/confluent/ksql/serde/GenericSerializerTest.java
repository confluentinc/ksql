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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericSerializerTest {

  private static final Map<String, ?> SOME_CONFIG = ImmutableMap.of("some", "thing");
  private static final byte[] SERIALIZED = "serialized".getBytes(StandardCharsets.UTF_8);

  @Mock
  private Serializer<List<?>> innerSerializer;
  private GenericSerializer<TestListWrapper> serializer;

  @Before
  public void setUp() {
    serializer = new GenericSerializer<>(TestListWrapper::getList, innerSerializer, 2);

    when(innerSerializer.serialize(any(), any())).thenReturn(SERIALIZED);
  }

  @Test
  public void shouldConfigureInnerSerializerOnConfigure() {
    // When:
    serializer.configure(SOME_CONFIG, true);

    // Then:
    verify(innerSerializer).configure(SOME_CONFIG, true);
  }

  @Test
  public void shouldCloseInnerSerializerOnClose() {
    // When:
    serializer.close();

    // Then:
    verify(innerSerializer).close();
  }

  @Test
  public void shouldSerializeNulls() {
    // When:
    final byte[] result = serializer.serialize("topic", null);

    // Then:
    verify(innerSerializer).serialize("topic", null);
    assertThat(result, is(SERIALIZED));
  }

  @Test
  public void shouldThrowOnSerializeOnColumnCountMismatch() {
    // Given:
   final TestListWrapper list = list("too", "many", "columns");

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("topicName", list)
    );

    // Then:
    assertThat(e.getMessage(), is("Column count mismatch on serialization."
        + " topic: topicName"
        + ", expected: 2"
        + ", got: 3"
    ));
  }

  @Test
  public void shouldConvertRowToListWhenSerializing() {
    // Given:
    final TestListWrapper list = list("hello", 10);

    // When:
    serializer.serialize("topicName", list);

    // Then:
    verify(innerSerializer).serialize("topicName", list.getList());
  }

  private static TestListWrapper list(final Object... values) {
    return new TestListWrapper(Arrays.asList(values));
  }

  public static final class TestListWrapper {

    private final List<?> values;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public TestListWrapper(final List<?> values) {
      this.values = values;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public List<?> getList() {
      return values;
    }
  }
}