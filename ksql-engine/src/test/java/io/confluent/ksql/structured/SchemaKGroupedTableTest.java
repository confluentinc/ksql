/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedTableTest {
  private static final LogicalSchema IN_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN1"), SqlTypes.INTEGER)
      .build();
  private static final LogicalSchema AGG_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("AGG0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("AGG1"), SqlTypes.BIGINT)
      .build();
  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT1"), SqlTypes.STRING)
      .build();
  private static final FunctionCall MIN = udaf("MIN");
  private static final FunctionCall MAX = udaf("MAX");
  private static final FunctionCall SUM = udaf("SUM");
  private static final FunctionCall COUNT = udaf("COUNT");

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
  private final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("GROUPING_COLUMN"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("AGG_VALUE"), SqlTypes.INTEGER)
      .build();
  private final MaterializedFactory materializedFactory = mock(MaterializedFactory.class);
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KTable table;

  @Before
  public void init() {
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(mockKGroupedTable.aggregate(any(), any(), any(), any())).thenReturn(table);
    when(table.mapValues(any(ValueMapper.class))).thenReturn(table);
  }

  private <S> ExecutionStep<S> buildSourceTableStep(final LogicalSchema schema) {
    final ExecutionStep<S> step = mock(ExecutionStep.class);
    when(step.getSchema()).thenReturn(schema);
    return step;
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    // Given:
    final WindowExpression windowExp = mock(WindowExpression.class);

    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Windowing not supported for table aggregations.");

    // When:
    groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    // Given:
    final SchemaKGroupedTable kGroupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The aggregation function(s) (MIN, MAX) cannot be applied to a table.");

    // When:
    kGroupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(MIN, MAX),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable(
      final KGroupedTable kGroupedTable,
      final MaterializedFactory materializedFactory
  ) {
    return new SchemaKGroupedTable(
        kGroupedTable,
        buildSourceTableStep(IN_SCHEMA),
        keyFormat,
        keySerde,
        KeyField.of(IN_SCHEMA.value().get(0).name(), IN_SCHEMA.value().get(0)),
        Collections.emptyList(),
        ksqlConfig,
        functionRegistry,
        materializedFactory);
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    final SchemaKGroupedTable kGroupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    final SchemaKTable result = kGroupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableAggregate(
                queryContext,
                kGroupedTable.getSourceTableStep(),
                OUT_SCHEMA,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(SUM, COUNT),
                AGG_SCHEMA
            )
        )
    );
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // Given:
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnColumnCountMismatch() {
    // Given:
    final SchemaKGroupedTable groupedTable =
        buildSchemaKGroupedTable(mockKGroupedTable, materializedFactory);

    // When:
    groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        2,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  private static FunctionCall udaf(final String name) {
    return new FunctionCall(
        FunctionName.of(name),
        ImmutableList.of(new ColumnReferenceExp(ColumnRef.of("IN1")))
    );
  }
}
