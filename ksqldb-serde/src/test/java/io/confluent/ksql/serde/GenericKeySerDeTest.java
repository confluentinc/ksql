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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes.SessionWindowedSerde;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericKeySerDeTest {

  private static final String LOGGER_PREFIX = "bob";
  private static final WindowInfo TIMED_WND = WindowInfo
      .of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10)));
  private static final WindowInfo SESSION_WND = WindowInfo
      .of(WindowType.SESSION, Optional.empty());

  @Mock
  private GenericSerdeFactory innerFactory;
  @Mock
  private FormatInfo format;
  @Mock
  private PersistenceSchema schema;
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingLogCxt;
  @Mock
  private Serde<List<?>> innerSerde;
  @Mock
  private Serializer<List<?>> innerSerializer;
  @Mock
  private Deserializer<List<?>> innerDeserializer;
  @Mock
  private Serde<Object> loggingSerde;

  private GenericKeySerDe factory;

  @Before
  public void setUp() {
    factory = new GenericKeySerDe(innerFactory);

    when(innerFactory.createFormatSerde(any(), any(), any(), any(), any())).thenReturn(innerSerde);
    when(innerFactory.wrapInLoggingSerde(any(), any(), any())).thenReturn(loggingSerde);

    when(innerSerde.serializer()).thenReturn(innerSerializer);
    when(innerSerde.deserializer()).thenReturn(innerDeserializer);
  }

  @Test
  public void shouldCreateInnerSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).createFormatSerde("Key", format, schema, config, srClientFactory);
  }

  @Test
  public void shouldCreateInnerSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).createFormatSerde("Key", format, schema, config, srClientFactory);
  }

  @Test
  public void shouldWrapInLoggingSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt));
  }

  @Test
  public void shouldWrapInLoggingSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt));
  }

  @Test
  public void shouldConfigureLoggingSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), true);
  }

  @Test
  public void shouldConfigureLoggingSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), true);
  }

  @Test
  public void shouldReturnLoggingSerdeNonWindowed() {
    // When:
    final Serde<Struct> result = factory
        .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    assertThat(result, is(sameInstance(loggingSerde)));
  }

  @Test
  public void shouldReturnedTimeWindowedSerdeForNonSessionWindowed() {
    // When:
    final Serde<Windowed<Struct>> result = factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    assertThat(result, is(instanceOf(TimeWindowedSerde.class)));
  }

  @Test
  public void shouldReturnedSessionWindowedSerdeForSessionWindowed() {
    // When:
    final Serde<Windowed<Struct>> result = factory
        .create(format, SESSION_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt);

    // Then:
    assertThat(result, is(instanceOf(SessionWindowedSerde.class)));
  }
}