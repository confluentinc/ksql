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

package io.confluent.ksql.logging.processing;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.LoggingDeserializer.DelayedResult;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingDeserializerTest {

  private static final GenericRow SOME_ROW = genericRow("some", "fields");
  private static final byte[] SOME_BYTES = "some bytes".getBytes(StandardCharsets.UTF_8);

  @Mock
  private Deserializer<GenericRow> delegate;
  @Mock
  private ProcessingLogger processingLogger;
  @Captor
  private ArgumentCaptor<Function<ProcessingLogConfig, SchemaAndValue>> errorCaptor;

  private LoggingDeserializer<GenericRow> deserializer;

  @Before
  public void setUp() {
    deserializer = new LoggingDeserializer<>(delegate, processingLogger);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester().testAllPublicConstructors(LoggingDeserializer.class);
  }

  @Test
  public void shouldConfigureDelegate() {
    // Given:
    final Map<String, ?> configs = ImmutableMap.of("some", "thing");

    // When:
    deserializer.configure(configs, true);

    // Then:
    verify(delegate).configure(configs, true);
  }

  @Test
  public void shouldCloseDelegate() {
    // When:
    deserializer.close();

    // Then:
    verify(delegate).close();
  }

  @Test
  public void shouldDeserializeWithDelegate() {
    // Given:
    when(delegate.deserialize(any(), any())).thenReturn(SOME_ROW);

    // When:
    deserializer.deserialize("some topic", SOME_BYTES);

    // Then:
    verify(delegate).deserialize("some topic", SOME_BYTES);
  }

  @Test
  public void shouldTryDeserializeWithDelegate() {
    // Given:
    when(delegate.deserialize(any(), any())).thenReturn(SOME_ROW);

    // When:
    final DelayedResult<GenericRow> result = deserializer.tryDeserialize("some topic", SOME_BYTES);

    // Then:
    verify(delegate).deserialize("some topic", SOME_BYTES);
    assertThat(result.isError(), is(false));
    assertThat(result.get(), is(SOME_ROW));
  }

  @Test(expected = ArithmeticException.class)
  public void shouldThrowIfDelegateThrows() {
    // Given:
    when(delegate.deserialize(any(), any())).thenThrow(new ArithmeticException());

    // When:
    deserializer.deserialize("t", SOME_BYTES);

    // Then: throws
  }

  @Test
  public void shouldLogOnException() {
    // Given:
    when(delegate.deserialize(any(), any()))
        .thenThrow(new RuntimeException("outer",
            new RuntimeException("inner", new RuntimeException("cause"))));

    // When:
    final RuntimeException e = assertThrows(
        RuntimeException.class,
        () -> deserializer.deserialize("t", SOME_BYTES)
    );

    // Then:
    verify(processingLogger).error(new DeserializationError(e, Optional.of(SOME_BYTES), "t"));
  }

  @Test
  public void shouldDelayLogOnException() {
    // Given:
    when(delegate.deserialize(any(), any()))
        .thenThrow(new RuntimeException("outer",
            new RuntimeException("inner", new RuntimeException("cause"))));

    // When:
    final DelayedResult<GenericRow> result = deserializer.tryDeserialize("t", SOME_BYTES);

    // Then:
    assertTrue(result.isError());
    assertThrows(RuntimeException.class, result::get);
    verify(processingLogger)
        .error(new DeserializationError(result.getError(), Optional.of(SOME_BYTES), "t"));
  }
}