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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
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

  private static final GenericRow SOME_ROW = new GenericRow(ImmutableList.of("some", "fields"));
  private static final byte[] SOME_BYTES = "some bytes".getBytes(StandardCharsets.UTF_8);
  private static final ProcessingLogConfig LOG_CONFIG = new ProcessingLogConfig(ImmutableMap.of(
      ProcessingLogConfig.INCLUDE_ROWS, true
  ));

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

  @Test(expected = ArithmeticException.class)
  public void shouldThrowIfDelegateThrows() {
    // Given:
    when(delegate.deserialize(any(), any())).thenThrow(new ArithmeticException());

    // When:
    deserializer.deserialize("t", SOME_BYTES);

    // Then: throws
  }

  @Test(expected = RuntimeException.class)
  public void shouldLogOnException() {
    // Given:
    when(delegate.deserialize(any(), any()))
        .thenThrow(new RuntimeException("outer",
            new RuntimeException("inner", new RuntimeException("cause"))));

    try {
      // When:
      deserializer.deserialize("t", SOME_BYTES);
    } catch (final RuntimeException e) {
      // Then:
      verify(processingLogger).error(errorCaptor.capture());

      final SchemaAndValue result = errorCaptor.getValue().apply(LOG_CONFIG);

      assertThat(result, is(SerdeProcessingLogMessageFactory
          .deserializationErrorMsg(e, Optional.of(SOME_BYTES)).apply(LOG_CONFIG)));

      throw e;
    }
  }
}