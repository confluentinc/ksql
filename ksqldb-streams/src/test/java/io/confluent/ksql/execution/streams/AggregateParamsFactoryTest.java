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

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.execution.streams.AggregateParamsFactory.KudafAggregatorFactory;
import io.confluent.ksql.execution.streams.AggregateParamsFactory.KudafUndoAggregatorFactory;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateParamsFactoryTest {

  private static final LogicalSchema INPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ARGUMENT0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ARGUMENT1"), SqlTypes.DOUBLE)
      .build();

  private static final List<ColumnName> NON_AGG_COLUMNS = ImmutableList.of(
      INPUT_SCHEMA.value().get(0).name(),
      INPUT_SCHEMA.value().get(2).name()
  );

  private static final FunctionCall AGG0 = new FunctionCall(
      FunctionName.of("AGG0"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final long INITIAL_VALUE0 = 123;
  private static final FunctionCall AGG1 = new FunctionCall(
      FunctionName.of("AGG1"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT1")))
  );
  private static final FunctionCall TABLE_AGG = new FunctionCall(
      FunctionName.of("TABLE_AGG"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("ARGUMENT0")))
  );
  private static final String INITIAL_VALUE1 = "initial";
  private static final List<FunctionCall> FUNCTIONS = ImmutableList.of(AGG0, AGG1);

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private AggregateFunctionFactory functionFactoryAggOne;
  @Mock
  private AggregateFunctionFactory functionFactoryAggTwo;
  @Mock
  private AggregateFunctionFactory functionFactoryTable;
  @Mock
  private KsqlAggregateFunction agg0;
  @Mock
  private KsqlAggregateFunction agg1;
  @Mock
  private TableAggregationFunction tableAgg;
  @Mock
  private KudafAggregatorFactory udafFactory;
  @Mock
  private KudafUndoAggregatorFactory undoUdafFactory;
  @Mock
  private KudafAggregator aggregator;
  @Mock
  private KudafUndoAggregator undoAggregator;

  private AggregateParams aggregateParams;

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(functionRegistry.getAggregateFactory(same(AGG0.getName())))
            .thenReturn(functionFactoryAggOne);
    when(functionFactoryAggOne.getFunction(any())).thenReturn(new AggregateFunctionFactory.FunctionSource(0, (initArgs) -> agg0));
    when(agg0.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE0);
    when(agg0.returnType()).thenReturn(SqlTypes.INTEGER);
    when(agg0.getAggregateType()).thenReturn(SqlTypes.BIGINT);
    when(functionRegistry.getAggregateFactory(same(AGG1.getName())))
            .thenReturn(functionFactoryAggTwo);
    when(functionFactoryAggTwo.getFunction(any())).thenReturn(new AggregateFunctionFactory.FunctionSource(0, (initArgs) -> agg1));
    when(agg1.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE1);
    when(agg1.returnType()).thenReturn(SqlTypes.STRING);
    when(agg1.getAggregateType()).thenReturn(SqlTypes.DOUBLE);
    when(functionRegistry.getAggregateFactory(same(TABLE_AGG.getName())))
            .thenReturn(functionFactoryTable);
    when(functionFactoryTable.getFunction(any())).thenReturn(new AggregateFunctionFactory.FunctionSource(0, (initArgs) -> tableAgg));
    when(tableAgg.getInitialValueSupplier()).thenReturn(() -> INITIAL_VALUE0);
    when(tableAgg.returnType()).thenReturn(SqlTypes.INTEGER);
    when(tableAgg.getAggregateType()).thenReturn(SqlTypes.BIGINT);

    when(udafFactory.create(anyInt(), any())).thenReturn(aggregator);
    when(undoUdafFactory.create(anyInt(), any())).thenReturn(undoAggregator);

    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).create(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        false,
        KsqlConfig.empty()
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateAggregatorWithCorrectParams() {
    verify(udafFactory).create(2, ImmutableList.of(agg0, agg1));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateUndoAggregatorWithCorrectParams() {
    // When:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).createUndoable(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        ImmutableList.of(TABLE_AGG),
        KsqlConfig.empty()
    );

    // Then:
    verify(undoUdafFactory).create(2, ImmutableList.of(tableAgg));
  }

  @Test
  public void shouldReturnCorrectAggregator() {
    // When:
    final KudafAggregator<Object> aggregator = aggregateParams.getAggregator();

    // Then:
    assertThat(aggregator, is(aggregator));
  }

  @Test
  public void shouldReturnCorrectInitializer() {
    // When:
    final KudafInitializer initializer = aggregateParams.getInitializer();

    // Then:
    assertThat(
        initializer.apply(),
        equalTo(genericRow(null, null, INITIAL_VALUE0, INITIAL_VALUE1))
    );
  }

  @Test
  public void shouldReturnEmptyUndoAggregator() {
    // When:
    final Optional<KudafUndoAggregator> undoAggregator = aggregateParams.getUndoAggregator();

    // Then:
    assertThat(undoAggregator.isPresent(), is(false));
  }

  @Test
  public void shouldReturnUndoAggregator() {
    // Given:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).createUndoable(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        ImmutableList.of(TABLE_AGG),
        KsqlConfig.empty()
    );

    // When:
    final KudafUndoAggregator undoAggregator = aggregateParams.getUndoAggregator().get();

    // Then:
    assertThat(undoAggregator, is(undoAggregator));
  }

  @Test
  public void shouldReturnCorrectAggregateSchema() {
    // When:
    final LogicalSchema schema = aggregateParams.getAggregateSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnNames.aggregateColumn(0), SqlTypes.BIGINT)
                .valueColumn(ColumnNames.aggregateColumn(1), SqlTypes.DOUBLE)
                .build()
        )
    );
  }

  @Test
  public void shouldReturnCorrectSchema() {
    // When:
    final LogicalSchema schema = aggregateParams.getSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnNames.aggregateColumn(0), SqlTypes.INTEGER)
                .valueColumn(ColumnNames.aggregateColumn(1), SqlTypes.STRING)
                .build()
        )
    );
  }

  @Test
  public void shouldReturnCorrectWindowedSchema() {
    // Given:
    aggregateParams = new AggregateParamsFactory(udafFactory, undoUdafFactory).create(
        INPUT_SCHEMA,
        NON_AGG_COLUMNS,
        functionRegistry,
        FUNCTIONS,
        true,
        KsqlConfig.empty()
    );

    // When:
    final LogicalSchema schema = aggregateParams.getSchema();

    // Then:
    assertThat(
        schema,
        equalTo(
            LogicalSchema.builder()
                .keyColumns(INPUT_SCHEMA.key())
                .valueColumn(ColumnName.of("REQUIRED0"), SqlTypes.BIGINT)
                .valueColumn(ColumnName.of("REQUIRED1"), SqlTypes.STRING)
                .valueColumn(ColumnNames.aggregateColumn(0), SqlTypes.INTEGER)
                .valueColumn(ColumnNames.aggregateColumn(1), SqlTypes.STRING)
                .valueColumn(SystemColumns.WINDOWSTART_NAME, SystemColumns.WINDOWBOUND_TYPE)
                .valueColumn(SystemColumns.WINDOWEND_NAME, SystemColumns.WINDOWBOUND_TYPE)
                .build()
        )
    );
  }
}
