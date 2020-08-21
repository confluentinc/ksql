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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedStreamTest {

  private static final LogicalSchema IN_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN1"), SqlTypes.INTEGER)
      .build();

  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("KSQL_AGG_VARIABLE_0"), SqlTypes.INTEGER)
      .build();

  private static final FunctionCall AGG = new FunctionCall(
      FunctionName.of("SUM"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ColumnName.of("IN1")))
  );

  private static final KsqlWindowExpression KSQL_WINDOW_EXP = new SessionWindowExpression(
      new WindowTimeClause(100, TimeUnit.SECONDS)
  );

  private static final List<ColumnName> NON_AGGREGATE_COLUMNS = ImmutableList.of(
      ColumnName.of("IN0")
  );

  @Mock
  private KsqlConfig config;
  @Mock
  private WindowExpression windowExp;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;
  @Mock
  private FormatInfo keyFormatInfo;
  @Mock
  private FormatInfo valueformatInfo;

  private final FunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");

  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    when(keyFormat.getFormatInfo()).thenReturn(keyFormatInfo);
    when(valueFormat.getFormatInfo()).thenReturn(valueformatInfo);
    schemaGroupedStream = new SchemaKGroupedStream(
        sourceStep,
        IN_SCHEMA,
        keyFormat,
        config,
        functionRegistry
    );
    when(windowExp.getKsqlWindowExpression()).thenReturn(KSQL_WINDOW_EXP);
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        NON_AGGREGATE_COLUMNS,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        NON_AGGREGATE_COLUMNS,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                Formats.of(keyFormat, valueFormat,
                    SerdeOptions.of()),
                NON_AGGREGATE_COLUMNS,
                ImmutableList.of(AGG)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForWindowedAggregate() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        NON_AGGREGATE_COLUMNS,
        ImmutableList.of(AGG),
        Optional.of(windowExp),
        valueFormat,
        queryContext
    );

    // Then:
    final KeyFormat expected = KeyFormat.windowed(
        FormatInfo.of(FormatFactory.KAFKA.name()),
        WindowInfo.of(WindowType.SESSION, Optional.empty())
    );
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamWindowedAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                Formats.of(expected, valueFormat, SerdeOptions.of()),
                NON_AGGREGATE_COLUMNS,
                ImmutableList.of(AGG),
                KSQL_WINDOW_EXP
            )
        )
    );
  }
}
