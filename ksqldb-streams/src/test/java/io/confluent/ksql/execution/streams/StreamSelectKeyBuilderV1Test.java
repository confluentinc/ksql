/*
 * Copyright 2021 Confluent Inc.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamSelectKeyV1;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamSelectKeyBuilderV1Test {

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .build()
      .withPseudoAndKeyColsInValue(false);

  private static final UnqualifiedColumnReferenceExp KEY =
      new UnqualifiedColumnReferenceExp(ColumnName.of("BOI"));

  private static final LogicalSchema RESULT_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BIG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("BOI"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of(SystemColumns.ROWTIME_NAME.text()), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of(SystemColumns.ROWPARTITION_NAME.text()), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of(SystemColumns.ROWOFFSET_NAME.text()), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("k0"), SqlTypes.DOUBLE)
      .build();

  private static final long A_BOI = 5000;
  private static final long A_BIG = 3000;

  private static final GenericKey SOURCE_KEY = GenericKey.genericKey("dre");

  @Mock
  private KStream<GenericKey, GenericRow> kstream;
  @Mock
  private KStream<GenericKey, GenericRow> rekeyedKstream;
  @Mock
  private KStream<GenericKey, GenericRow> filteredKStream;
  @Mock
  private ExecutionStep<KStreamHolder<GenericKey>> sourceStep;
  @Mock
  private RuntimeBuildContext buildContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private PlanInfo planInfo;
  @Captor
  private ArgumentCaptor<Predicate<GenericKey, GenericRow>> predicateCaptor;
  @Captor
  private ArgumentCaptor<KeyValueMapper<GenericKey, GenericRow, GenericKey>> keyValueMapperCaptor;

  private final QueryContext queryContext =
      new QueryContext.Stacker().push("ya").getQueryContext();

  private PlanBuilder planBuilder;
  private StreamSelectKeyV1 selectKey;

  @Before
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void init() {
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.getKsqlConfig()).thenReturn(new KsqlConfig(ImmutableMap.of()));
    when(kstream.filter(any())).thenReturn(filteredKStream);
    when(filteredKStream.selectKey(any(KeyValueMapper.class))).thenReturn(rekeyedKstream);
    when(sourceStep.build(any(), eq(planInfo))).thenReturn(
        new KStreamHolder<>(kstream, SOURCE_SCHEMA, mock(ExecutionKeyFactory.class)));
    planBuilder = new KSPlanBuilder(
        buildContext,
        mock(SqlPredicateFactory.class),
        mock(AggregateParamsFactory.class),
        mock(StreamsFactories.class)
    );
    selectKey = new StreamSelectKeyV1(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        KEY
    );
  }

  @Test
  public void shouldRekeyCorrectly() {
    // When:
    final KStreamHolder<GenericKey> result = selectKey.build(planBuilder, planInfo);

    // Then:
    final InOrder inOrder = Mockito.inOrder(kstream, filteredKStream, rekeyedKstream);
    inOrder.verify(kstream).filter(any());
    inOrder.verify(filteredKStream).selectKey(any());
    inOrder.verifyNoMoreInteractions();
    assertThat(result.getStream(), is(rekeyedKstream));
  }

  @Test
  public void shouldReturnCorrectSerdeFactory() {
    // When:
    final KStreamHolder<GenericKey> result = selectKey.build(planBuilder, planInfo);

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

  @Test
  public void shouldFilterOutNullValues() {
    // When:
    selectKey.build(planBuilder, planInfo);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(SOURCE_KEY, null), is(false));
  }

  @Test
  public void shouldFilterOutNullKeyColumns() {
    // When:
    selectKey.build(planBuilder, planInfo);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = getPredicate();
    assertThat(
        predicate.test(SOURCE_KEY, value(A_BIG, null, 0, "dre")),
        is(false)
    );
  }

  @Test
  public void shouldNotFilterOutNonNullKeyColumns() {
    // When:
    selectKey.build(planBuilder, planInfo);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = getPredicate();
    assertThat(
        predicate.test(SOURCE_KEY, value(A_BIG, A_BOI, 0, "dre")),
        is(true)
    );
  }

  @Test
  public void shouldIgnoreNullNonKeyColumns() {
    // When:
    selectKey.build(planBuilder, planInfo);

    // Then:
    verify(kstream).filter(predicateCaptor.capture());
    final Predicate<GenericKey, GenericRow> predicate = getPredicate();
    assertThat(predicate.test(SOURCE_KEY, value(null, A_BOI, 0, "dre")), is(true));
  }

  @Test
  public void shouldComputeCorrectKey() {
    // When:
    selectKey.build(planBuilder, planInfo);

    // Then:
    final KeyValueMapper<GenericKey, GenericRow, GenericKey> keyValueMapper = getKeyMapper();
    assertThat(
        keyValueMapper.apply(SOURCE_KEY, value(A_BIG, A_BOI, 0, "dre")),
        is(GenericKey.genericKey(A_BOI))
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final KStreamHolder<GenericKey> result = selectKey.build(planBuilder, planInfo);

    // Then:
    assertThat(result.getSchema(), is(RESULT_SCHEMA));
  }

  private KeyValueMapper<GenericKey, GenericRow, GenericKey> getKeyMapper() {
    verify(filteredKStream).selectKey(keyValueMapperCaptor.capture());
    return keyValueMapperCaptor.getValue();
  }

  private Predicate<GenericKey, GenericRow> getPredicate() {
    verify(kstream).filter(predicateCaptor.capture());
    return predicateCaptor.getValue();
  }

  private static GenericRow value(
      final Long big,
      final Long boi,
      final int rowTime,
      final String rowKey
  ) {
    return GenericRow.genericRow(big, boi, rowTime, rowKey);
  }
}
