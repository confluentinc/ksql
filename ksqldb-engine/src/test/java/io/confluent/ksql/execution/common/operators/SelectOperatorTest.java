/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.common.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.common.operators.SelectOperator;
import io.confluent.ksql.execution.streams.SqlPredicateFactory;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectOperatorTest {
  private static final LogicalSchema OUTPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema INTERMEDIATE_SCHEMA_WITH_PSEUDO = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema WINDOWED_OUTPUT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT)
      .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey("k");
  private static final long A_ROWTIME = 12335L;

  private static final Window A_WINDOW = Window.of(Instant.now(), Instant.now().plusMillis(10));

  @Mock
  private ProcessingLogger logger;
  @Mock
  private AbstractPhysicalOperator child;
  @Mock
  private KsqlTransformer<Object, Optional<GenericRow>> transformer;
  @Mock
  private SqlPredicateFactory predicateFactory;
  @Mock
  private SqlPredicate sqlPredicate;
  @Mock
  private QueryFilterNode logicalNode;

  @Test
  public void shouldSelectKeyNonWindowed() {
    // Given:
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getIntermediateSchema()).thenReturn(INTERMEDIATE_SCHEMA_WITH_PSEUDO);
    when(predicateFactory.create(any(), any())).thenReturn(sqlPredicate);
    final SelectOperator selectOperator = new SelectOperator(
        logicalNode,
        logger,
        predicateFactory);
    selectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        OUTPUT_SCHEMA,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(sqlPredicate.getTransformer(logger)).thenReturn(transformer);
    final QueryRowImpl intermediateRow = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(transformer.transform(A_KEY, intermediateRow.value(), new PullProcessingContext(12335L)))
        .thenReturn(Optional.of(GenericRow.genericRow("a", "b", A_ROWTIME, "k")));
    selectOperator.open();

    // When:
    Object result = selectOperator.next();

    // Then:
    assertThat(result, is(intermediateRow));
  }

  @Test
  public void shouldSelectKeyWindowed() {
    // Given:
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getIntermediateSchema()).thenReturn(WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO);
    when(predicateFactory.create(any(), any())).thenReturn(sqlPredicate);
    final SelectOperator selectOperator = new SelectOperator(
        logicalNode,
        logger,
        predicateFactory);
    selectOperator.addChild(child);
    final QueryRowImpl windowedRow = QueryRowImpl.of(
        WINDOWED_OUTPUT_SCHEMA,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(sqlPredicate.getTransformer(logger)).thenReturn(transformer);
    final QueryRowImpl intermediateWindowedRow = QueryRowImpl.of(
        WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        A_ROWTIME
    );
    when(transformer.transform(A_KEY, intermediateWindowedRow.value(), new PullProcessingContext(12335L)))
        .thenReturn(Optional.of(GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli())));
    selectOperator.open();

    // When:
    Object result = selectOperator.next();

    // Then:
    assertThat(result, is(intermediateWindowedRow));
  }

}
