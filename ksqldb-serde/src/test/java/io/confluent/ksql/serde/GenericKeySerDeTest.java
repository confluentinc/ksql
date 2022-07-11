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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.tracked.TrackedCallback;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
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
  private static final String queryId = "query";
  private static final WindowInfo TIMED_WND = WindowInfo
      .of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10)), Optional.empty());
  private static final WindowInfo SESSION_WND = WindowInfo
      .of(WindowType.SESSION, Optional.empty(), Optional.empty());
  private static final ColumnName COLUMN_NAME = ColumnName.of("foo");

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
  @Mock
  private Serde<Object> trackingSerde;
  @Mock
  private TrackedCallback callback;

  private GenericKeySerDe factory;

  @Before
  public void setUp() {
    factory = new GenericKeySerDe(innerFactory, Optional.of(queryId));

    when(innerFactory.createFormatSerde(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(innerSerde);
    when(innerFactory.wrapInLoggingSerde(any(), any(), any(), any())).thenReturn(loggingSerde);
    when(innerFactory.wrapInTrackingSerde(any(), any())).thenReturn(trackingSerde);

    when(innerSerde.serializer()).thenReturn(innerSerializer);
    when(innerSerde.deserializer()).thenReturn(innerDeserializer);
  }

  @Test
  public void shouldCreateInnerSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory).createFormatSerde("Key", format, schema, config, srClientFactory, true);
  }

  @Test
  public void shouldCreateInnerSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    verify(innerFactory).createFormatSerde("Key", format, schema, config, srClientFactory, true);
  }

  @Test
  public void shouldWrapInLoggingSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt), eq(Optional.of(queryId)));
  }

  @Test
  public void shouldWrapInLoggingSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    verify(innerFactory).wrapInLoggingSerde(any(), eq(LOGGER_PREFIX), eq(processingLogCxt), eq(Optional.of(queryId)));
  }

  @Test
  public void shouldConfigureLoggingSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), true);
  }

  @Test
  public void shouldConfigureLoggingSerdeWindowed() {
    // When:
    factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    verify(loggingSerde).configure(ImmutableMap.of(), true);
  }

  @Test
  public void shouldReturnLoggingSerdeNonWindowed() {
    // When:
    final Serde<GenericKey> result = factory
        .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    assertThat(result, is(sameInstance(loggingSerde)));
  }

  @Test
  public void shouldReturnTrackingSerdeNonWindowed() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.of(callback));

    // Then:
    verify(innerFactory).wrapInTrackingSerde(loggingSerde, callback);
  }

  @Test
  public void shouldReturnTrackingSerdeWindowed() {
    // When:
    factory.create(format, SESSION_WND, schema, config, srClientFactory, LOGGER_PREFIX,
        processingLogCxt, Optional.of(callback));

    // Then:
    verify(innerFactory).wrapInTrackingSerde(loggingSerde, callback);
  }

  @Test
  public void shouldNotWrapInTrackingSerdeIfNoCallbackProvided() {
    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then:
    verify(innerFactory, never()).wrapInTrackingSerde(any(), any());
  }

  @Test
  public void shouldReturnedTimeWindowedSerdeForNonSessionWindowed() {
    // When:
    final Serde<Windowed<GenericKey>> result = factory
        .create(format, TIMED_WND, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then:
    assertThat(result, is(instanceOf(TimeWindowedSerde.class)));
  }

  @Test
  public void shouldReturnedSessionWindowedSerdeForSessionWindowed() {
    // When:
    final Serde<Windowed<GenericKey>> result = factory
        .create(format, SESSION_WND, schema, config, srClientFactory, LOGGER_PREFIX,
            processingLogCxt,
            Optional.empty());

    // Then:
    assertThat(result, is(instanceOf(SessionWindowedSerde.class)));
  }

  @Test
  public void shouldNotThrowOnNoKeyColumns() {
    // Given:
    schema = PersistenceSchema.from(ImmutableList.of(), SerdeFeatures.of());

    // When:
    factory
        .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
            Optional.empty());

    // Then (did not throw):
  }

  @Test
  public void shouldNotThrowOnMultipleKeyColumns() {
    // Given:
    schema = PersistenceSchema.from(
        ImmutableList.of(column(SqlTypes.STRING), column(SqlTypes.INTEGER)),
        SerdeFeatures.of()
    );

    // When:
    factory.create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
        Optional.empty());

    // Then (did not throw):
  }

  @Test
  public void shouldThrowOnMapKeyColumn() {
    // Given:
    schema = PersistenceSchema.from(
        ImmutableList.of(column(SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))),
        SerdeFeatures.of()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory
            .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
                Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Map keys, including types that contain maps, are not supported as they may lead "
            + "to unexpected behavior due to inconsistent serialization. Key column name: `foo`. "
            + "Column type: MAP<STRING, STRING>. "
            + "See https://github.com/confluentinc/ksql/issues/6621 for more."));
  }

  @Test
  public void shouldThrowOnNestedMapKeyColumn() {
    // Given:
    schema = PersistenceSchema.from(
        ImmutableList.of(column(SqlTypes.struct()
            .field("F", SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
            .build())),
        SerdeFeatures.of()
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory
            .create(format, schema, config, srClientFactory, LOGGER_PREFIX, processingLogCxt,
                Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Map keys, including types that contain maps, are not supported as they may lead "
            + "to unexpected behavior due to inconsistent serialization. Key column name: `foo`. "
            + "Column type: STRUCT<`F` MAP<STRING, STRING>>. "
            + "See https://github.com/confluentinc/ksql/issues/6621 for more."));
  }

  private static SimpleColumn column(final SqlType type) {
    final SimpleColumn column = mock(SimpleColumn.class, type.toString());
    when(column.name()).thenReturn(COLUMN_NAME);
    when(column.type()).thenReturn(type);
    return column;
  }
}