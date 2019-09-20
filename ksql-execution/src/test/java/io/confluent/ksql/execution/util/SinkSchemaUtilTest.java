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

package io.confluent.ksql.execution.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SinkSchemaUtilTest {
  @Mock
  private ExecutionStep step;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private void givenStepWithSchema(final LogicalSchema schema) {
    when(step.getSources()).thenReturn(ImmutableList.of(step));
    when(step.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(schema, mock(QueryContext.class))
    );
  }

  @Test
  public void shouldComputeIndexesToRemoveImplicitsAndRowKey() {
    // Given:
    givenStepWithSchema(LogicalSchema.builder()
        .valueColumn("field1", SqlTypes.STRING)
        .valueColumn("field2", SqlTypes.STRING)
        .valueColumn("field3", SqlTypes.STRING)
        .valueColumn("timestamp", SqlTypes.BIGINT)
        .valueColumn("key", SqlTypes.STRING)
        .build()
        .withMetaAndKeyColsInValue()
    );

    // When:
    final Set<Integer> indices = SinkSchemaUtil.implicitAndKeyColumnIndexesInValueSchema(step);

    // Then:
    assertThat(indices, contains(0, 1));
  }

  @Test
  public void shouldComputeIndexesToRemoveImplicitsAndRowKeyRegardlessOfLocation() {
    // Given:
    givenStepWithSchema(LogicalSchema.builder()
        .valueColumn("field1", SqlTypes.STRING)
        .valueColumn("field2", SqlTypes.STRING)
        .valueColumn("ROWKEY", SqlTypes.STRING)
        .valueColumn("field3", SqlTypes.STRING)
        .valueColumn("timestamp", SqlTypes.BIGINT)
        .valueColumn("ROWTIME", SqlTypes.BIGINT)
        .valueColumn("key", SqlTypes.STRING)
        .build()
    );

    // When:
    final Set<Integer> indices = SinkSchemaUtil.implicitAndKeyColumnIndexesInValueSchema(step);

    // Then:
    assertThat(indices, contains(2, 5));
  }
}