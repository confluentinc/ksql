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

package io.confluent.ksql.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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
      LogicalSchema.of(SchemaBuilder.struct()
          .field("f0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .build()),
      SerdeOption.none()
  );

  private static final QueryId QUERY_ID = new QueryId("fred");

  private static final FormatInfo FORMAT_INFO = FormatInfo
      .of(Format.AVRO, Optional.of("io.confluent.ksql"));

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
  private KeySerde<Struct> keySerde;
  @Mock
  private KeySerde<Windowed<Struct>> windowedKeySerde;
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

    queryContext = new QueryContext.Stacker(QUERY_ID).push("context").getQueryContext();

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
    // Given:
    final PlanNodeId planNodeId = new PlanNodeId("some-id");

    // When:
    final Stacker result = ksqlQueryBuilder.buildNodeContext(planNodeId);

    // Then:
    assertThat(result, is(new Stacker(QUERY_ID).push("some-id")));
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
        QueryLoggerUtil.queryLoggerName(queryContext),
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
        QueryLoggerUtil.queryLoggerName(queryContext),
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
        QueryLoggerUtil.queryLoggerName(queryContext),
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
    assertThat(ksqlQueryBuilder.getSchemas().toString(),
        is("fred.context = STRUCT<f0 BOOLEAN> NOT NULL"));
  }

  @Test
  public void shouldTrackSchemasTakingIntoAccountSerdeOptions() {
    // Given:
    final PhysicalSchema schema = PhysicalSchema.from(
        SOME_SCHEMA.logicalSchema(),
        SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)
    );

    // When:
    ksqlQueryBuilder.buildValueSerde(
        FORMAT_INFO,
        schema,
        queryContext
    );

    // Then:
    assertThat(ksqlQueryBuilder.getSchemas().toString(),
        is("fred.context = BOOLEAN"));
  }
}