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
import static org.hamcrest.Matchers.instanceOf;
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
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryLoggerUtil;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
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
  private KsqlSerdeFactory valueSerdeFactory;
  @Mock
  private Serde<Object> rowSerde;
  @Mock
  private Serializer<Object> innerSerializer;
  @Mock
  private Deserializer<Object> innerDeserializer;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  private QueryContext queryContext;
  private KsqlQueryBuilder ksqlQueryBuilder;

  @Before
  public void setUp() {
    when(rowSerde.serializer()).thenReturn(innerSerializer);
    when(rowSerde.deserializer()).thenReturn(innerDeserializer);
    when(valueSerdeFactory.createSerde(any(), any(), any())).thenReturn(rowSerde);
    when(serviceContext.getSchemaRegistryClientFactory()).thenReturn(srClientFactory);

    queryContext = new QueryContext.Stacker(QUERY_ID).push("context").getQueryContext();

    final ProcessingLoggerFactory loggerFactory = mock(ProcessingLoggerFactory.class);
    final ProcessingLogger logger = mock(ProcessingLogger.class);
    when(loggerFactory.getLogger(any())).thenReturn(logger);
    when(processingLogContext.getLoggerFactory()).thenReturn(loggerFactory);

    ksqlQueryBuilder = KsqlQueryBuilder.of(
        streamsBuilder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        QUERY_ID
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
  public void shouldBuildGenericRowSerde() {
    // When:
    final Serde<GenericRow> result = ksqlQueryBuilder.buildGenericRowSerde(
        valueSerdeFactory,
        SOME_SCHEMA,
        queryContext
    );

    // Then:
    verify(valueSerdeFactory).createSerde(
        SOME_SCHEMA.valueSchema(),
        ksqlConfig,
        srClientFactory
    );

    final Serde<GenericRow> expected = GenericRowSerDe.from(
        valueSerdeFactory,
        SOME_SCHEMA,
        ksqlConfig,
        srClientFactory,
        QueryLoggerUtil.queryLoggerName(queryContext),
        processingLogContext
    );

    assertThat(result.serializer(), is(instanceOf(expected.serializer().getClass())));
    assertThat(result.deserializer(), is(instanceOf(expected.deserializer().getClass())));
  }

  @Test
  public void shouldTrackSchemasUsed() {
    // When:
    ksqlQueryBuilder.buildGenericRowSerde(
        valueSerdeFactory,
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
    ksqlQueryBuilder.buildGenericRowSerde(
        valueSerdeFactory,
        schema,
        queryContext
    );

    // Then:
    assertThat(ksqlQueryBuilder.getSchemas().toString(),
        is("fred.context = BOOLEAN"));
  }
}