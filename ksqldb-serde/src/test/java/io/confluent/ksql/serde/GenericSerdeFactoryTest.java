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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.SchemaNotSupportedException;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericSerdeFactoryTest {

  @Mock
  private Function<FormatInfo, Format> formatFactory;

  @Mock
  private Format format;
  @Mock
  private FormatInfo formatInfo;
  @Mock(name = "schemaText")
  private PersistenceSchema schema;
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private Map<String, String> formatProperties;
  @Mock
  private Serde<String> formatSerde;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ProcessingLogger processingLoggerWithoutQueryId;
  @Mock
  private ProcessingLogger processingLoggerWithQueryId;
  @Mock
  private Serializer<String> formatSerializer;
  @Mock
  private Deserializer<String> formatDeserializer;

  private GenericSerdeFactory serdeFactory;

  @Before
  public void setUp() {
    serdeFactory = new GenericSerdeFactory(formatFactory);

    when(formatFactory.apply(any())).thenReturn(format);
    when(formatInfo.getProperties()).thenReturn(formatProperties);
    when(format.name()).thenReturn("FormatName");

    when(formatSerde.serializer()).thenReturn(formatSerializer);
    when(formatSerde.deserializer()).thenReturn(formatDeserializer);

    when(formatSerializer.serialize(any(), any()))
        .thenThrow(new RuntimeException("serializer error"));

    when(formatDeserializer.deserialize(any(), any()))
        .thenThrow(new RuntimeException("deserializer error"));

    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger(any())).thenReturn(processingLoggerWithoutQueryId);
    when(processingLoggerFactory.getLogger(any(), any())).thenReturn(processingLoggerWithQueryId);
  }

  @Test
  public void shouldInvokeFormatFactoryWithCorrectParams() {
    // When:
    serdeFactory.createFormatSerde("target", formatInfo, schema, config, srClientFactory, false);

    // Then:
    verify(formatFactory).apply(formatInfo);
  }

  @Test
  public void shouldThrowIfFormatFactoryThrows() {
    // Given:
    final RuntimeException expected = mock(RuntimeException.class);

    when(formatFactory.apply(any())).thenThrow(expected);

    // When:
    final Exception actual = assertThrows(
        RuntimeException.class,
        () -> serdeFactory
            .createFormatSerde("target", formatInfo, schema, config, srClientFactory, false)
    );

    // Then:
    assertThat(actual, is(sameInstance(expected)));
  }

  @Test
  public void shouldInvokeFormatWithCorrectParams() {
    // When:
    serdeFactory.createFormatSerde("target", formatInfo, schema, config, srClientFactory, false);

    // Then:
    verify(format).getSerde(schema, formatProperties, config, srClientFactory, false);
  }

  @Test
  public void shouldThrowIfGetSerdeThrows() {
    // Given:
    when(format.getSerde(any(), any(), any(), any(), eq(false))).thenThrow(new RuntimeException("boom"));

    // When:
    final Exception actual = assertThrows(
        SchemaNotSupportedException.class,
        () -> serdeFactory
            .createFormatSerde("Target-A", formatInfo, schema, config, srClientFactory, false)
    );

    // Then:
    assertThat(actual.getMessage(), is("Target-A format does not support schema."
        + System.lineSeparator()
        + "format: FormatName"
        + System.lineSeparator()
        + "schema: schemaText"
        + System.lineSeparator()
        + "reason: boom"
    ));
  }

  @Test
  public void shouldWrapSerializerWithLoggingSerializer() {
    // Given:
    final Serde<String> result = serdeFactory
        .wrapInLoggingSerde(formatSerde, "prefix", processingLogContext);

    // Then:
    assertThrows(
        RuntimeException.class,
        () -> result.serializer().serialize(
            "topicName",
            "this call will cause error to be logged"
        )
    );

    verify(processingLoggerWithoutQueryId).error(any());
  }

  @Test
  public void shouldWrapDeserializerWithLoggingSerializer() {
    // Given:
    when(processingLoggerFactory.getLogger("prefix.deserializer")).thenReturn(processingLoggerWithoutQueryId);

    // When:
    final Serde<String> result = serdeFactory
        .wrapInLoggingSerde(formatSerde, "prefix", processingLogContext);

    // Then:
    assertThrows(
        RuntimeException.class,
        () -> result.deserializer().deserialize(
            "topicName",
            "this call will cause error to be logged".getBytes(StandardCharsets.UTF_8)
        )
    );

    verify(processingLoggerWithoutQueryId).error(any());
  }

  @Test
  public void shouldReturnProcessingLoggerWithQueryId() {
   // When:
    final Serde<String> result = serdeFactory
        .wrapInLoggingSerde(formatSerde, "prefix", processingLogContext, Optional.of("query-id"));

    // Then:
    assertThrows(
        RuntimeException.class,
        () -> result.deserializer().deserialize(
            "topicName",
            "this call will cause error to be logged".getBytes(StandardCharsets.UTF_8)
        )
    );

    verify(processingLoggerWithQueryId).error(any());
  }
}