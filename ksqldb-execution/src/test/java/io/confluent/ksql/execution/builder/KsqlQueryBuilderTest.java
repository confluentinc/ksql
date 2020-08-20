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

package io.confluent.ksql.execution.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class KsqlQueryBuilderTest {

  private static final PhysicalSchema SOME_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
          .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
          .build(),
      SerdeOption.none()
  );

  private static final QueryId QUERY_ID = new QueryId("fred");

  private static final FormatInfo FORMAT_INFO = FormatInfo
      .of(FormatFactory.AVRO.name(), ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "io.confluent.ksql"));

  private static final WindowInfo WINDOW_INFO = WindowInfo
      .of(WindowType.TUMBLING, Optional.of(Duration.ofMillis(1000)));

  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private Serde<Struct> keySerde;
  @Mock
  private Serde<Windowed<Struct>> windowedKeySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private KeySerdeFactory keySerdeFactory;
  @Mock
  private ValueSerdeFactory valueSerdeFactory;
  private QueryContext queryContext;
  private KsqlQueryBuilder ksqlQueryBuilder;


  @Before
  public void setUp() {
    when(serviceContext.getSchemaRegistryClientFactory()).thenReturn(srClientFactory);

    queryContext = new QueryContext.Stacker().push("context").getQueryContext();

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(keySerde);

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(windowedKeySerde);

    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any()))
        .thenReturn(valueSerde);

    ksqlQueryBuilder = new KsqlQueryBuilder(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        QUERY_ID,
        keySerdeFactory,
        valueSerdeFactory
    );
  }

  @Test
  public void shouldThrowNPEOnConstruction() {
    new NullPointerTester()
        .setDefault(StreamsBuilder.class, streamsBuilder)
        .setDefault(KsqlConfig.class, ksqlConfig)
        .setDefault(ServiceContext.class, serviceContext)
        .setDefault(ProcessingLogContext.class, processingLogContext)
        .setDefault(FunctionRegistry.class, functionRegistry)
        .setDefault(QueryId.class, QUERY_ID)
        .testAllPublicStaticMethods(KsqlQueryBuilder.class);
  }

  @Test
  public void shouldBuildNodeContext() {
    // When:
    final Stacker result = ksqlQueryBuilder.buildNodeContext("some-id");

    // Then:
    assertThat(result, is(new Stacker().push("some-id")));
  }

  @Test
  public void shouldSwapInKsqlConfig() {
    // Given:
    final KsqlConfig other = mock(KsqlConfig.class);

    // When:
    final KsqlQueryBuilder result = ksqlQueryBuilder.withKsqlConfig(other);

    // Then:
    assertThat(ksqlQueryBuilder.getKsqlConfig(), is(ksqlConfig));
    assertThat(result.getKsqlConfig(), is(other));
  }

  @Test
  public void shouldBuildNonWindowedKeySerde() {
    // Then:
    ksqlQueryBuilder.buildKeySerde(
        FORMAT_INFO,
        SOME_SCHEMA,
        queryContext
    );

    // Then:
    verify(keySerdeFactory).create(
        FORMAT_INFO,
        SOME_SCHEMA.keySchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext
    );
  }

  @Test
  public void shouldBuildWindowedKeySerde() {
    // Then:
    ksqlQueryBuilder.buildKeySerde(
        FORMAT_INFO,
        WINDOW_INFO,
        SOME_SCHEMA,
        queryContext
    );

    // Then:
    verify(keySerdeFactory).create(
        FORMAT_INFO,
        WINDOW_INFO,
        SOME_SCHEMA.keySchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext
    );
  }

  @Test
  public void shouldBuildValueSerde() {
    // Then:
    ksqlQueryBuilder.buildValueSerde(
        FORMAT_INFO,
        SOME_SCHEMA,
        queryContext
    );

    // Then:
    verify(valueSerdeFactory).create(
        FORMAT_INFO,
        SOME_SCHEMA.valueSchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext
    );
  }

  @Test
  public void shouldTrackSchemasUsed() {
    // When:
    ksqlQueryBuilder.buildValueSerde(
        FORMAT_INFO,
        SOME_SCHEMA,
        queryContext
    );

    // Then:
    final Map<String, PhysicalSchema> schemas = ksqlQueryBuilder.getSchemas().getSchemas();
    assertThat(schemas.entrySet(), hasSize(1));
    assertThat(schemas.get("fred.context"), is(SOME_SCHEMA));
  }
}