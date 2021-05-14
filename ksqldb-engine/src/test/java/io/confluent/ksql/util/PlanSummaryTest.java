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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlanSummaryTest {

  private static final QueryId QUERY_ID = new QueryId("QID");

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("L0"), SqlTypes.INTEGER)
      .build();

  @Mock
  private StepSchemaResolver schemaResolver;

  private ExecutionStep<?> sourceStep;
  private PlanSummary planSummaryBuilder;

  @Before
  public void setup() {
    planSummaryBuilder = new PlanSummary(QUERY_ID, schemaResolver);
    sourceStep = givenStep(StreamSource.class, "src", SOURCE_SCHEMA);
  }

  @Test
  public void shouldSummarizeSource() {
    // When:
    final String summary = planSummaryBuilder.summarize(sourceStep);

    // Then:
    assertThat(summary, is(
        " > [ SOURCE ] | Schema: ROWKEY STRING KEY, L0 INTEGER | Logger: QID.src\n"
    ));
  }

  @Test
  public void shouldSummarizeWithSource() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("L1"), SqlTypes.STRING)
        .build();

    final ExecutionStep<?> step = givenStep(StreamSelect.class, "child", schema, sourceStep);

    // When:
    final String summary = planSummaryBuilder.summarize(step);

    // Then:
    assertThat(summary, is(
        " > [ PROJECT ] | Schema: ROWKEY STRING KEY, L1 STRING | Logger: QID.child"
            + "\n\t\t > [ SOURCE ] | Schema: ROWKEY STRING KEY, L0 INTEGER | Logger: QID.src\n"
    ));
  }

  @Test
  public void shouldSummarizePlanWithMultipleSources() {
    // Given:
    final LogicalSchema sourceSchema2 = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("L0_2"), SqlTypes.STRING)
        .build();

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("L1"), SqlTypes.STRING)
        .build();

    final ExecutionStep<?> sourceStep2 = givenStep(StreamSource.class, "src2", sourceSchema2);
    final ExecutionStep<?> step =
        givenStep(StreamStreamJoin.class, "child", schema, sourceStep, sourceStep2);

    // When:
    final String summary = planSummaryBuilder.summarize(step);

    // Then:
    assertThat(summary, is(
        " > [ JOIN ] | Schema: ROWKEY STRING KEY, L1 STRING | Logger: QID.child"
            + "\n\t\t > [ SOURCE ] | Schema: ROWKEY STRING KEY, L0 INTEGER | Logger: QID.src"
            + "\n\t\t > [ SOURCE ] | Schema: ROWKEY STRING KEY, L0_2 STRING | Logger: QID.src2\n"
    ));
  }

  @Test
  public void shouldThrowOnUnsupportedStepType() {
    // Given:
    final ExecutionStep<?> step = mock(ExecutionStep.class);

    // When:
    assertThrows(
        UnsupportedOperationException.class,
        () -> planSummaryBuilder.summarize(step)
    );
  }

  private <T extends ExecutionStep<?>> T givenStep(
      final Class<T> clazz,
      final String ctx,
      final LogicalSchema schema,
      final ExecutionStep<?> ...sources) {
    final T step = mock(clazz);
    givenStep(step, ctx, schema, sources);
    return step;
  }

  private void givenStep(
      final ExecutionStep<?> step,
      final String ctx,
      final LogicalSchema schema,
      final ExecutionStep<?> ...sources) {
    final ExecutionStepPropertiesV1 props = mock(ExecutionStepPropertiesV1.class);
    when(step.getProperties()).thenReturn(props);
    when(props.getQueryContext())
        .thenReturn(new QueryContext.Stacker().push(ctx).getQueryContext());
    when(schemaResolver.resolve(same(step), any())).thenReturn(schema);
    when(schemaResolver.resolve(same(step), any(), any())).thenReturn(schema);
    when(step.getSources()).thenReturn(Arrays.asList(sources));
  }
}