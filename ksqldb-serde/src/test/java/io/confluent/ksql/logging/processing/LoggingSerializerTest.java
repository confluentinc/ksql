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

package io.confluent.ksql.logging.processing;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.GenericRow;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingSerializerTest {

  private static final GenericRow SOME_ROW = genericRow("some", "fields");
  private static final byte[] SOME_BYTES = "some bytes".getBytes(StandardCharsets.UTF_8);
  private static final Exception ERROR = new RuntimeException("outer",
      new RuntimeException("inner", new RuntimeException("cause")));

  @Mock
  private Serializer<GenericRow> delegate;
  @Mock
  private ProcessingLogger processingLogger;

  private LoggingSerializer<GenericRow> serializer;

  @Before
  public void setUp() {
    serializer = new LoggingSerializer<>(delegate, processingLogger);
    serializer.configure(Collections.emptyMap(), false);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester().testAllPublicConstructors(LoggingSerializer.class);
  }

  @Test
  public void shouldConfigureDelegate() {
    // Given:
    final Map<String, ?> configs = ImmutableMap.of("some", "thing");

    // When:
    serializer.configure(configs, true);

    // Then:
    verify(delegate).configure(configs, true);
  }

  @Test
  public void shouldCloseDelegate() {
    // When:
    serializer.close();

    // Then:
    verify(delegate).close();
  }

  @Test
  public void shouldSerializeWithDelegate() {
    // Given:
    when(delegate.serialize(any(), any())).thenReturn(SOME_BYTES);

    // When:
    serializer.serialize("some topic", SOME_ROW);

    // Then:
    verify(delegate).serialize("some topic", SOME_ROW);
  }

  @Test
  public void shouldThrowIfDelegateThrows() {
    // Given:
    when(delegate.serialize(any(), any())).thenThrow(ERROR);

    // When:
    final RuntimeException e = assertThrows(
        RuntimeException.class,
        () -> serializer.serialize("t", SOME_ROW)
    );

    // Then:
    assertThat(e, is(ERROR));
  }

  @Test
  public void shouldLogOnException() {
    // Given:
    when(delegate.serialize(any(), any())).thenThrow(ERROR);

    // When:
    assertThrows(
        RuntimeException.class,
        () -> serializer.serialize("t", SOME_ROW)
    );

    // Then:
    verify(processingLogger).error(new SerializationError<>(ERROR, Optional.of(SOME_ROW), "t", false));
  }

  @Test
  public void shouldLogExceptionForKey() {
    // Given:
    serializer.configure(Collections.emptyMap(), true);

    when(delegate.serialize(any(), any())).thenThrow(ERROR);

    // When:
    assertThrows(
        RuntimeException.class,
        () -> serializer.serialize("t", SOME_ROW)
    );

    // Then:
    verify(processingLogger).error(new SerializationError<>(ERROR, Optional.of(SOME_ROW), "t", true));
  }

}