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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
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
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
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

  private static final List<Expression> KEY =
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("BOI")));

  private static final LogicalSchema RESULT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of(SystemColumns.ROWTIME_NAME.text()), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of(SystemColumns.ROWPARTITION_NAME.text()), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of(SystemColumns.ROWOFFSET_NAME.text()), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .build();

  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KStream<GenericKey, GenericRow> kstream;
  @Mock
  private KStream<GenericKey, GenericRow> rekeyedKstream;
  @Mock
  private ExecutionStep<KStreamHolder<GenericKey>> sourceStep;
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private KStreamHolder<GenericKey> stream;
  @Mock
  private PartitionByParamsBuilder paramBuilder;
  @Mock
  private PartitionByParams<GenericKey> params;
  @Mock
  private PartitionByParams.Mapper<GenericKey> mapper;
  @Mock
  private GenericKey aKey;
  @Mock
  private GenericRow aValue;
  @Mock
  private ExecutionKeyFactory<GenericKey> keyFactory;
  @Captor
  private ArgumentCaptor<KeyValueMapper<GenericKey, GenericRow, KeyValue<GenericKey, GenericRow>>> mapperCaptor;
  @Captor
  private ArgumentCaptor<Named> nameCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker().push("ya").getQueryContext();

  private StreamSelectKey<GenericKey> selectKey;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(buildContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.getKsqlConfig()).thenReturn(CONFIG);

    when(paramBuilder.build(any(), eq(keyFactory), any(), any(), any(), any())).thenReturn(params);

    when(params.getMapper()).thenReturn(mapper);
    when(params.getSchema()).thenReturn(RESULT_SCHEMA);

    when(stream.getStream()).thenReturn(kstream);
    when(stream.getSchema()).thenReturn(SOURCE_SCHEMA);

    when(stream.getExecutionKeyFactory()).thenReturn(keyFactory);
    when(keyFactory.withQueryBuilder(any())).thenReturn(keyFactory);
    when(keyFactory.buildKeySerde(any(), any(), any()))
        .thenAnswer(inv -> buildContext.buildKeySerde(
            inv.getArgument(0), inv.getArgument(1), inv.getArgument(2)));

    when(kstream.map(any(KeyValueMapper.class), any(Named.class))).thenReturn(rekeyedKstream);

    selectKey = new StreamSelectKey<>(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        KEY
    );
  }

  @Test
  public void shouldPassCorrectArgsToParamBuilder() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    verify(paramBuilder).build(
        SOURCE_SCHEMA,
        stream.getExecutionKeyFactory(),
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
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    verify(kstream).map(mapperCaptor.capture(), any());

    final KeyValueMapper<GenericKey, GenericRow, KeyValue<GenericKey, GenericRow>> result =
        mapperCaptor.getValue();

    // When:
    result.apply(aKey, aValue);

    // Then:
    verify(mapper).apply(aKey, aValue);
  }

  @Test
  public void shouldUseCorrectNameInMapCall() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    verify(kstream).map(any(), nameCaptor.capture());

    assertThat(NamedTestAccessor.getName(nameCaptor.getValue()), is("ya-SelectKey"));
  }

  @Test
  public void shouldOnlyMap() {
    // When:
    StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    verify(kstream).map(any(), any());
    verifyNoMoreInteractions(kstream, rekeyedKstream);
  }

  @Test
  public void shouldReturnRekeyedStream() {
    // When:
    final KStreamHolder<GenericKey> result = StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    assertThat(result.getStream(), is(rekeyedKstream));
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<GenericKey> result = StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    assertThat(result.getSchema(), is(RESULT_SCHEMA));
  }

  @Test
  public void shouldReturnCorrectSerdeFactory() {
    // When:
    final KStreamHolder<GenericKey> result = StreamSelectKeyBuilder
        .build(stream, selectKey, buildContext, paramBuilder);

    // Then:
    result.getExecutionKeyFactory().buildKeySerde(
        FormatInfo.of(FormatFactory.JSON.name()),
        PhysicalSchema.from(SOURCE_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of()),
        queryContext
    );

    verify(buildContext).buildKeySerde(
        FormatInfo.of(FormatFactory.JSON.name()),
        PhysicalSchema.from(SOURCE_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of()),
        queryContext);
  }
}
