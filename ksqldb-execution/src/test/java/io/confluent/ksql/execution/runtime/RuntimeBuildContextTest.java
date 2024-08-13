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

package io.confluent.ksql.execution.runtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.query.QuerySchemas.SchemaInfo;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class RuntimeBuildContextTest {

  protected static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema.from(
      LOGICAL_SCHEMA,
      SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
      SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)
  );

  private static final QueryId QUERY_ID = new QueryId("fred");

  private static final FormatInfo FORMAT_INFO = FormatInfo
      .of(FormatFactory.AVRO.name(),
          ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "io.confluent.ksql"));

  private static final WindowInfo WINDOW_INFO = WindowInfo
      .of(WindowType.TUMBLING, Optional.of(Duration.ofMillis(1000)), Optional.empty());

  @Mock
  private StreamsBuilder streamsBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private ProcessingLoggerFactory processingLogFactory;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private Serde<GenericKey> keySerde;
  @Mock
  private Serde<Windowed<GenericKey>> windowedKeySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private KeySerdeFactory keySerdeFactory;
  @Mock
  private ValueSerdeFactory valueSerdeFactory;
  private QueryContext queryContext;
  private RuntimeBuildContext runtimeBuildContext;

  @Before
  public void setUp() {
    when(serviceContext.getSchemaRegistryClientFactory()).thenReturn(srClientFactory);

    queryContext = new QueryContext.Stacker().push("context").getQueryContext();

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(keySerde);

    when(keySerdeFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(windowedKeySerde);

    when(valueSerdeFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(valueSerde);

    when(processingLogContext.getLoggerFactory()).thenReturn(processingLogFactory);

    runtimeBuildContext = new RuntimeBuildContext(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        "foo",
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
        .testAllPublicStaticMethods(RuntimeBuildContext.class);
  }

  @Test
  public void shouldBuildNonWindowedKeySerde() {
    // Then:
    runtimeBuildContext.buildKeySerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    verify(keySerdeFactory).create(
        FORMAT_INFO,
        PHYSICAL_SCHEMA.keySchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext,
        Optional.empty()
    );
  }

  @Test
  public void shouldBuildWindowedKeySerde() {
    // Then:
    runtimeBuildContext.buildKeySerde(
        FORMAT_INFO,
        WINDOW_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    verify(keySerdeFactory).create(
        FORMAT_INFO,
        WINDOW_INFO,
        PHYSICAL_SCHEMA.keySchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext,
        Optional.empty()
    );
  }

  @Test
  public void shouldBuildValueSerde() {
    // Then:
    runtimeBuildContext.buildValueSerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    verify(valueSerdeFactory).create(
        FORMAT_INFO,
        PHYSICAL_SCHEMA.valueSchema(),
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext),
        processingLogContext,
        Optional.empty()
    );
  }

  @Test
  public void shouldTrackKeySchemasUsed() {
    // When:
    runtimeBuildContext.buildKeySerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    final Map<String, SchemaInfo> schemas = runtimeBuildContext.getSchemas().getLoggerSchemaInfo();
    assertThat(schemas.entrySet(), hasSize(1));
    assertThat(schemas.get("fred.context"), is(new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(KeyFormat.nonWindowed(
            FormatInfo.of("AVRO", ImmutableMap.of("fullSchemaName", "io.confluent.ksql")),
            SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))),
        Optional.empty()
    )));
  }

  @Test
  public void shouldTrackValueSchemasUsed() {
    // When:
    runtimeBuildContext.buildValueSerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    final Map<String, SchemaInfo> schemas = runtimeBuildContext.getSchemas().getLoggerSchemaInfo();
    assertThat(schemas.entrySet(), hasSize(1));
    assertThat(schemas.get("fred.context"), is(new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.empty(),
        Optional.of(ValueFormat.of(
            FormatInfo.of("AVRO", ImmutableMap.of("fullSchemaName", "io.confluent.ksql")),
            SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)))
    )));
  }

  @Test
  public void shouldMergeKeyAndValueSchemaInfo() {
    // When:
    runtimeBuildContext.buildKeySerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    runtimeBuildContext.buildValueSerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    // Then:
    final Map<String, SchemaInfo> schemas = runtimeBuildContext.getSchemas().getLoggerSchemaInfo();
    assertThat(schemas.entrySet(), hasSize(1));
    assertThat(schemas.get("fred.context"), is(new SchemaInfo(
        LOGICAL_SCHEMA,
        Optional.of(KeyFormat.nonWindowed(
            FormatInfo.of("AVRO", ImmutableMap.of("fullSchemaName", "io.confluent.ksql")),
            SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES))),
        Optional.of(ValueFormat.of(
            FormatInfo.of("AVRO", ImmutableMap.of("fullSchemaName", "io.confluent.ksql")),
            SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)))
    )));
  }

  @Test
  public void shouldFailWhenTackingSerdeOnSchemaMismatch() {
    // Given:
    runtimeBuildContext.buildKeySerde(
        FORMAT_INFO,
        PHYSICAL_SCHEMA,
        queryContext
    );

    final PhysicalSchema differentSchema = PhysicalSchema.from(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("f0"), SqlTypes.BOOLEAN)
            .build(),
        SerdeFeatures.of(),
        SerdeFeatures.of()
    );

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> runtimeBuildContext.buildValueSerde(
            FORMAT_INFO,
            differentSchema,
            queryContext
        ));
  }

  @Test
  public void shouldReturnProcessingLogger() {
    // When:
    when(processingLogFactory.getLogger(
        QueryLoggerUtil.queryLoggerName(QUERY_ID, queryContext), Collections.singletonMap("query-id", QUERY_ID.toString())))
        .thenReturn(processingLogger);
    final ProcessingLogger logger = runtimeBuildContext.getProcessingLogger(queryContext);

    // Then:
    assertThat(processingLogger, equalTo(logger));
  }
}