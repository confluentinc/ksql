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

import static io.confluent.ksql.function.UserFunctionLoaderTestUtil.loadAllUserFunctions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedTableTest {

  private static final LogicalSchema IN_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN1"), SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("KSQL_AGG_VARIABLE_0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("KSQL_AGG_VARIABLE_1"), SqlTypes.BIGINT)
      .build();

  private static final List<ColumnName> NON_AGG_COLUMNS = ImmutableList.of(
      ColumnName.of("IN0")
  );

  private static final FunctionCall MIN = udaf("MIN");
  private static final FunctionCall MAX = udaf("MAX");
  private static final FunctionCall SUM = udaf("SUM");
  private static final FunctionCall COUNT = udaf("COUNT");
  private static final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final ValueFormat valueFormat = ValueFormat
      .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());
  private KeyFormat keyFormat = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());

  @BeforeClass
  public static void setUpFunctionRegistry() {
    loadAllUserFunctions(functionRegistry);
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    // Given:
    final WindowExpression windowExp = mock(WindowExpression.class);

    final SchemaKGroupedTable groupedTable = buildSchemaKGroupedTable();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> groupedTable.aggregate(
            NON_AGG_COLUMNS,
            ImmutableList.of(SUM, COUNT),
            Optional.of(windowExp),
            valueFormat.getFormatInfo(),
            queryContext
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Windowing not supported for table aggregations."));
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    // Given:
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTable();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> kGroupedTable.aggregate(
            NON_AGG_COLUMNS,
            ImmutableList.of(MIN, MAX),
            Optional.empty(),
            valueFormat.getFormatInfo(),
            queryContext
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("The aggregation functions MIN and MAX cannot be applied to a table source, only to a stream source."));
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable() {
    return new SchemaKGroupedTable(
        mock(ExecutionStep.class),
        IN_SCHEMA,
        keyFormat,
        ksqlConfig,
        functionRegistry
    );
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    keyFormat = KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of());

    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTable();

    // When:
    final SchemaKTable result = kGroupedTable.aggregate(
        NON_AGG_COLUMNS,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat.getFormatInfo(),
        queryContext
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableAggregate(
                queryContext,
                kGroupedTable.getSourceTableStep(),
                Formats.of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(), SerdeFeatures.of(), SerdeFeatures.of()),
                NON_AGG_COLUMNS,
                ImmutableList.of(SUM, COUNT)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForAggregateWithKeyFormatSerdeFeatures() {
    // Given:
    keyFormat = KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES));

    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTable();

    // When:
    final SchemaKTable result = kGroupedTable.aggregate(
        NON_AGG_COLUMNS,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat.getFormatInfo(),
        queryContext
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableAggregate(
                queryContext,
                kGroupedTable.getSourceTableStep(),
                Formats.of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(), SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES), SerdeFeatures.of()),
                NON_AGG_COLUMNS,
                ImmutableList.of(SUM, COUNT)
            )
        )
    );
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // Given:
    final SchemaKGroupedTable groupedTable = buildSchemaKGroupedTable();

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        NON_AGG_COLUMNS,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat.getFormatInfo(),
        queryContext
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  private static FunctionCall udaf(final String name) {
    return new FunctionCall(
        FunctionName.of(name),
        ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("IN1")))
    );
  }
}
