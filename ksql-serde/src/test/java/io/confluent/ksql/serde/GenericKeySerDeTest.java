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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.GenericKeySerDe.UnwrappedKeySerializer;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericKeySerDeTest {

  private static final FormatInfo FORMAT = FormatInfo.of(Format.JSON);
  private static final WindowInfo WINDOW = WindowInfo.of(WindowType.SESSION, Optional.empty());
  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());
  private static final String LOGGER_NAME_PREFIX = "bob";

  private static final PersistenceSchema WRAPPED_SCHEMA = PersistenceSchema.from(
      (ConnectSchema) SchemaBuilder
          .struct()
          .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
          .build(),
      false
  );

  private static final PersistenceSchema UNWRAPPED_SCHEMA = PersistenceSchema.from(
      (ConnectSchema) SchemaBuilder
          .struct()
          .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
          .build(),
      true
  );

  @Mock
  private SerdeFactories serdeFactories;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingLogCxt;
  @Mock
  private ProcessingLoggerFactory loggerFactory;
  @Mock
  private ProcessingLogger logger;
  @Mock
  private Serde<Object> innerSerde;
  @Mock
  private Serializer<Object> innerSerializer;
  @Mock
  private Deserializer<Object> innerDeserializer;
  private GenericKeySerDe factory;

  @Before
  public void setUp() {
    factory = new GenericKeySerDe(serdeFactories);

    when(processingLogCxt.getLoggerFactory()).thenReturn(loggerFactory);
    when(loggerFactory.getLogger(any())).thenReturn(logger);

    when(serdeFactories.create(any(), any(), any(), any(), any())).thenReturn(innerSerde);

    when(innerSerde.serializer()).thenReturn(innerSerializer);
    when(innerSerde.deserializer()).thenReturn(innerDeserializer);
  }

  @Test
  public void shouldCreateCorrectInnerSerdeForWrapped() {
    // When:
    factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Struct.class
    );
  }

  @Test
  public void shouldCreateCorrectInnerSerdeForUnwrapped() {
    // When:
    factory.create(
        FORMAT,
        UNWRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        UNWRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Long.class
    );
  }

  @Test
  public void shouldCreateProcessLoggerWithCorrectName() {
    // When:
    factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    verify(loggerFactory).getLogger("bob.deserializer");
  }

  @Test
  public void shouldUseInnerSerializerForWrappedSchema() {
    // When:
    final KeySerde<Struct> result = factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.serializer(), is(innerSerializer));
  }

  @Test
  public void shouldUseUnwrappingSerializerForUnwrappedSchema() {
    // When:
    final KeySerde<Struct> result = factory.create(
        FORMAT,
        UNWRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.serializer(), is(instanceOf(UnwrappedKeySerializer.class)));
  }

  @Test
  public void shouldUseLoggingDeserializer() {
    // When:
    final KeySerde<Struct> result = factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.deserializer(), is(instanceOf(LoggingDeserializer.class)));
  }

  @Test
  public void shouldUseSessionWindowedSerde() {
    // When:
    final KeySerde<Windowed<Struct>> result = factory.create(
        FORMAT,
        WindowInfo.of(WindowType.SESSION, Optional.empty()),
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.serializer(), is(instanceOf(SessionWindowedSerializer.class)));
    assertThat(result.deserializer(), is(instanceOf(SessionWindowedDeserializer.class)));
  }

  @Test
  public void shouldUseTimeWindowedSerdeForHopping() {
    // When:
    final KeySerde<Windowed<Struct>> result = factory.create(
        FORMAT,
        WindowInfo.of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10))),
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.serializer(), is(instanceOf(TimeWindowedSerializer.class)));
    assertThat(result.deserializer(), is(instanceOf(TimeWindowedDeserializer.class)));
  }

  @Test
  public void shouldUseTimeWindowedSerdeForTumbling() {
    // When:
    final KeySerde<Windowed<Struct>> result = factory.create(
        FORMAT,
        WindowInfo.of(WindowType.TUMBLING, Optional.of(Duration.ofMinutes(10))),
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    assertThat(result.serializer(), is(instanceOf(TimeWindowedSerializer.class)));
    assertThat(result.deserializer(), is(instanceOf(TimeWindowedDeserializer.class)));
  }

  @Test
  public void shouldConfigureInnerSerde() {
    // When:
    factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // Then:
    verify(innerSerializer).configure(Collections.emptyMap(), true);
    verify(innerDeserializer).configure(Collections.emptyMap(), true);
  }

  @Test
  public void shouldCloseInnerSerde() {
    // Given:
    final KeySerde<Struct> keySerde = factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // When:
    keySerde.close();

    // Then:
    verify(innerSerializer).close();
    verify(innerDeserializer).close();
  }

  @Test
  public void shouldRebindNoneWindowedToNewSchema() {
    // Given:
    final KeySerde<Struct> keySerde = factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    // When:
    final KeySerde<Struct> rebound = keySerde.rebind(UNWRAPPED_SCHEMA);

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        UNWRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Long.class
    );

    assertThat(rebound.isWindowed(), is(false));
    assertThat(rebound.serializer(), is(instanceOf(UnwrappedKeySerializer.class)));
  }

  @Test
  public void shouldRebindWindowedToNewSchema() {
    // Given:
    final KeySerde<Windowed<Struct>> keySerde = factory.create(
        FORMAT,
        WINDOW,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    clearInvocations(serdeFactories);

    // When:
    final KeySerde<Struct> rebound = keySerde.rebind(UNWRAPPED_SCHEMA);

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        UNWRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Long.class
    );

    assertThat(rebound.isWindowed(), is(false));
    assertThat(rebound.serializer(), is(instanceOf(UnwrappedKeySerializer.class)));
  }

  @Test
  public void shouldRebindNoneWindowedToWindow() {
    // Given:
    final KeySerde<Struct> keySerde = factory.create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    clearInvocations(serdeFactories);

    // When:
    final KeySerde<Windowed<Struct>> rebound = keySerde.rebind(WINDOW);

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Struct.class
    );

    assertThat(rebound.isWindowed(), is(true));
    assertThat(rebound.serializer(), is(instanceOf(SessionWindowedSerializer.class)));
  }

  @Test
  public void shouldRebindWindowedToWindow() {
    // Given:
    final KeySerde<Windowed<Struct>> keySerde = factory.create(
        FORMAT,
        WINDOW,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        LOGGER_NAME_PREFIX,
        processingLogCxt
    );

    clearInvocations(serdeFactories);

    // When:
    final KeySerde<Windowed<Struct>> rebound = keySerde
        .rebind(WindowInfo.of(WindowType.TUMBLING, Optional.of(Duration.ofSeconds(10))));

    // Then:
    verify(serdeFactories).create(
        FORMAT,
        WRAPPED_SCHEMA,
        CONFIG,
        srClientFactory,
        Struct.class
    );

    assertThat(rebound.isWindowed(), is(true));
    assertThat(rebound.serializer(), is(instanceOf(TimeWindowedSerializer.class)));
  }
}