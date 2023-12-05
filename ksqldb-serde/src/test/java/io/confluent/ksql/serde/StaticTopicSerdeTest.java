/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.logging.processing.DeserializationError;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StaticTopicSerdeTest {

  private static final byte[] SOME_BYTES = new byte[]{1, 2, 3};
  private static final Object SOME_OBJECT = 1;
  private static final String STATIC_TOPIC = "static";
  private static final String SOURCE_TOPIC = "source";

  @Mock
  private Serializer<Object> delegateS;
  @Mock
  private Deserializer<Object> delegateD;
  @Mock
  private Callback callback;

  private Serde<Object> staticSerde;

  @Before
  public void setUp() {
    final Serde<Object> delegate = new WrapperSerde<>(delegateS, delegateD);

    when(delegateS.serialize(Mockito.any(), Mockito.any())).thenReturn(SOME_BYTES);
    when(delegateD.deserialize(Mockito.any(), Mockito.any())).thenReturn(SOME_OBJECT);

    staticSerde = StaticTopicSerde.wrap(STATIC_TOPIC, delegate, callback);
  }

  @Test
  public void shouldUseDelegateSerializerWithStaticTopic() {
    // When:
    final byte[] serialized = staticSerde.serializer().serialize(SOURCE_TOPIC, SOME_OBJECT);

    // Then:
    verify(delegateS).serialize(STATIC_TOPIC, SOME_OBJECT);
    assertThat(serialized, is(SOME_BYTES));
    verifyNoMoreInteractions(callback);
  }

  @Test
  public void shouldUseDelegateDeserializerWithStaticTopic() {
    // When:
    final Object deserialized = staticSerde.deserializer().deserialize(SOURCE_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateD).deserialize(STATIC_TOPIC, SOME_BYTES);
    assertThat(deserialized, is(SOME_OBJECT));
    verifyNoMoreInteractions(callback);
  }

  @Test
  public void shouldUseDelegateLoggingDeserializerWithStaticTopic() {
    // Given:
    final ProcessingLogger logger = mock(ProcessingLogger.class);
    final LoggingDeserializer<Object> loggingDelegate = new LoggingDeserializer<>(delegateD, logger);
    final Serde<Object> delegate = new WrapperSerde<>(delegateS, loggingDelegate);

    staticSerde = StaticTopicSerde.wrap(STATIC_TOPIC, delegate, callback);

    // When:
    final Object deserialized = staticSerde.deserializer().deserialize(SOURCE_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateD).deserialize(STATIC_TOPIC, SOME_BYTES);
    assertThat(deserialized, is(SOME_OBJECT));
    verifyNoMoreInteractions(callback);
    verifyNoMoreInteractions(logger);
  }

  @Test
  public void shouldTrySourceTopicAndCallCallbackOnDeserializationFailure() {
    // Given:
    when(delegateD.deserialize(Mockito.eq(STATIC_TOPIC), Mockito.any())).thenThrow(new RuntimeException());

    // When:
    final Object deserialized = staticSerde.deserializer().deserialize(SOURCE_TOPIC, SOME_BYTES);

    // Then:
    verify(delegateD).deserialize(STATIC_TOPIC, SOME_BYTES);
    verify(delegateD).deserialize(SOURCE_TOPIC, SOME_BYTES);
    verify(callback).onDeserializationFailure(SOURCE_TOPIC, STATIC_TOPIC, SOME_BYTES);
    assertThat(deserialized, is(SOME_OBJECT));
  }

  @Test
  public void shouldTrySourceTopicAndCallCallbackOnDeserializationFailureWithLoggingDeserializer() {
    // Given:
    when(delegateD.deserialize(Mockito.eq(STATIC_TOPIC), Mockito.any())).thenThrow(new RuntimeException());

    final ProcessingLogger logger = mock(ProcessingLogger.class);
    final LoggingDeserializer<Object> loggingDelegate = new LoggingDeserializer<>(delegateD, logger);
    final Serde<Object> delegate = new WrapperSerde<>(delegateS, loggingDelegate);

    staticSerde = StaticTopicSerde.wrap(STATIC_TOPIC, delegate, callback);

    // When:
    final Object deserialized = staticSerde.deserializer().deserialize(SOURCE_TOPIC, SOME_BYTES);

    // Then:
    verifyNoMoreInteractions(logger);
    verify(callback).onDeserializationFailure(SOURCE_TOPIC, STATIC_TOPIC, SOME_BYTES);
    assertThat(deserialized, is(SOME_OBJECT));
  }

  @Test
  public void shouldLogOriginalFailureIfBothFail() {
    // Given:
    when(delegateD.deserialize(Mockito.any(), Mockito.any())).thenThrow(new RuntimeException());

    final ProcessingLogger logger = mock(ProcessingLogger.class);
    final LoggingDeserializer<Object> loggingDelegate = new LoggingDeserializer<>(delegateD, logger);
    final Serde<Object> delegate = new WrapperSerde<>(delegateS, loggingDelegate);

    staticSerde = StaticTopicSerde.wrap(STATIC_TOPIC, delegate, callback);

    // When:
    final RuntimeException err = assertThrows(
        RuntimeException.class,
        () -> staticSerde.deserializer().deserialize(SOURCE_TOPIC, SOME_BYTES));

    // Then:
    verify(logger).error(new DeserializationError(err, Optional.of(SOME_BYTES), STATIC_TOPIC, false));
    verifyNoMoreInteractions(callback);
  }

}