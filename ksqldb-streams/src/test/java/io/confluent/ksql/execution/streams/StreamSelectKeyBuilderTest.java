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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.streams.StreamSelectKeyBuilder.PartitionByParamsBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.NamedTestAccessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamSelectKeyBuilderTest {

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final UnqualifiedColumnReferenceExp KEY =
      new UnqualifiedColumnReferenceExp(ColumnName.of("BOI"));

  private static final LogicalSchema RESULT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of(SystemColumns.ROWTIME_NAME.text()), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .build();

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KStream<Struct, GenericRow> kstream;
  @Mock
  private KStream<Struct, GenericRow> rekeyedKstream;
  @Mock
  private ExecutionStep<KStreamHolder<Struct>> sourceStep;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KStreamHolder<Struct> stream;
  @Mock
  private PartitionByParamsBuilder paramBuilder;
  @Mock
  private PartitionByParams params;
  @Mock
  private BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper;
  @Mock
  private Struct aKey;
  @Mock
  private GenericRow aValue;
  @Captor
  private ArgumentCaptor<KeyValueMapper<Struct, GenericRow, KeyValue<Struct, GenericRow>>> mapperCaptor;
  @Captor
  private ArgumentCaptor<Named> nameCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker().push("ya").getQueryContext();

  private StreamSelectKey selectKey;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getProcessingLogger(any())).thenReturn(processingLogger);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getKsqlConfig()).thenReturn(CONFIG);

    when(paramBuilder.build(any(), any(), any(), any(), any())).thenReturn(params);

    when(params.getMapper()).thenReturn(mapper);
    when(params.getSchema()).thenReturn(RESULT_SCHEMA);

    when(stream.getStream()).thenReturn(kstream);
    when(stream.getSchema()).thenReturn(SOURCE_SCHEMA);

    when(kstream.map(any(KeyValueMapper.class), any(Named.class))).thenReturn(rekeyedKstream);

    selectKey = new StreamSelectKey(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        KEY
    );
  }

  @Test
  public void shouldPassCorrectArgsToParamBuilder() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    verify(paramBuilder).build(
        SOURCE_SCHEMA,
        KEY,
        CONFIG,
        functionRegistry,
        processingLogger
    );
  }

  @Test
  public void shouldUserMapperInMapCall() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    verify(kstream).map(mapperCaptor.capture(), any());

    final KeyValueMapper<Struct, GenericRow, KeyValue<Struct, GenericRow>> result = mapperCaptor
        .getValue();

    // When:
    result.apply(aKey, aValue);

    // Then:
    verify(mapper).apply(aKey, aValue);
  }

  @Test
  public void shouldUseCorrectNameInMapCall() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    verify(kstream).map(any(), nameCaptor.capture());

    assertThat(NamedTestAccessor.getName(nameCaptor.getValue()), is("ya-SelectKey"));
  }

  @Test
  public void shouldOnlyMap() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    verify(kstream).map(any(), any());
    verifyNoMoreInteractions(kstream, rekeyedKstream);
  }

  @Test
  public void shouldReturnRekeyedStream() {
    // When:
    final KStreamHolder<Struct> result = StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    assertThat(result.getStream(), is(rekeyedKstream));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<Struct> result = StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    assertThat(result.getSchema(), is(RESULT_SCHEMA));
  }

  @Test
  public void shouldReturnCorrectSerdeFactory() {
    // When:
    final KStreamHolder<Struct> result = StreamSelectKeyBuilder
        .build(stream, selectKey, queryBuilder, paramBuilder);

    // Then:
    result.getKeySerdeFactory().buildKeySerde(
        FormatInfo.of(FormatFactory.JSON.name()),
        PhysicalSchema.from(SOURCE_SCHEMA, SerdeOptions.of()),
        queryContext
    );

    verify(queryBuilder).buildKeySerde(
        FormatInfo.of(FormatFactory.JSON.name()),
        PhysicalSchema.from(SOURCE_SCHEMA, SerdeOptions.of()),
        queryContext);
  }
}
