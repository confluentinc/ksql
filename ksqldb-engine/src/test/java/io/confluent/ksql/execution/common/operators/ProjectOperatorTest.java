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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory.SelectValueMapperFactorySupplier;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProjectOperatorTest {

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
      .keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT)
      .keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT)
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
  private SelectValueMapperFactorySupplier selectValueMapperFactorySupplier;
  @Mock
  private QueryProjectNode logicalNode;
  @Mock
  private AbstractPhysicalOperator child;
  @Mock
  private KsqlTransformer<Object, GenericRow> transformer;
  @Mock
  private SelectValueMapper<Object> selectValueMapper;

  @Test
  public void shouldProjectAllColumnsWhenSelectStarNonWindowed() {
    // Given:
    when(logicalNode.getSchema()).thenReturn(OUTPUT_SCHEMA);
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, row.value()))
        .thenReturn(GenericRow.genericRow("k", "a", "b"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    final List<Object> expected = new ArrayList<>(row.key().values());
    expected.add("a");
    expected.add("b");
    assertThat(result.value().values(), is(expected));
  }

  @Test
  public void shouldProjectAllColumnsWhenSelectStarWindowed() {
    // Given:
    when(logicalNode.getSchema()).thenReturn(WINDOWED_OUTPUT_SCHEMA);
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl windowedRow = QueryRowImpl.of(
        WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, windowedRow.value()))
        .thenReturn(GenericRow.genericRow("k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli(), "a", "b"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    final List<Object> expected = new ArrayList<>(windowedRow.key().values());
    expected.add(windowedRow.window().get().start().toEpochMilli());
    expected.add(windowedRow.window().get().end().toEpochMilli());
    expected.add("a");
    expected.add("b");
    assertThat(result.value().values(), is(expected));
  }

  @Test
  public void shouldCallTransformWithCorrectArguments() {
    // Given:
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(OUTPUT_SCHEMA);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(any(), any())).thenReturn(GenericRow.genericRow("k", "a", "b"));
    projectOperator.open();

    // When:
    projectOperator.next();

    // Then:
    verify(transformer).transform(
        A_KEY, GenericRow.genericRow("a", "b", 12335L, "k", 12335L, "k"));
  }

  @Test
  public void shouldCallTransformWithCorrectArgumentsWindowed() {
    // Given:
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(OUTPUT_SCHEMA);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl windowedRow = QueryRowImpl.of(
        WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(any(), any())).thenReturn(GenericRow.genericRow("k", "a", "b"));
    projectOperator.open();

    // When:
    projectOperator.next();

    // Then:
    verify(transformer).transform(
        A_KEY,
        GenericRow.genericRow("a", "b", 12335L, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli(),
                              12335L, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli())
    );
  }

  @Test
  public void shouldProjectOnlyKeyNonWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .build();
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(schema);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, row.value()))
             .thenReturn(GenericRow.genericRow("k"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    assertThat(result.value().values(), is(row.key().values()));
  }

  @Test
  public void shouldProjectOnlyValueNonWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .build();
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(false);
    when(logicalNode.getSchema()).thenReturn(schema);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, row.value()))
        .thenReturn(GenericRow.genericRow("b"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    assertThat(result.value().values(), is(ImmutableList.of("b")));
  }

  @Test
  public void shouldProjectOnlyWindowStartWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT)
        .build();
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(schema);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl windowedRow = QueryRowImpl.of(
        WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, windowedRow.value()))
        .thenReturn(GenericRow.genericRow(A_WINDOW.start().toEpochMilli()));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    assertThat(result.value().values(), is(ImmutableList.of(A_WINDOW.start().toEpochMilli())));
  }


  @Test
  public void shouldProjectKeyAndValueNonWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .build();
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(schema);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl row = QueryRowImpl.of(
        INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.empty(),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, row.value()))
        .thenReturn(GenericRow.genericRow("k","b"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    final List<Object> expected = new ArrayList<>(row.key().values());
    expected.add(row.value().values().get(1));
    assertThat(result.value().values(), is(expected));
  }

  @Test
  public void shouldProjectKeyAndValueWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .build();
    when(logicalNode.getAddAdditionalColumnsToIntermediateSchema()).thenReturn(true);
    when(logicalNode.getSchema()).thenReturn(schema);
    when(logicalNode.getCompiledSelectExpressions()).thenReturn(Collections.emptyList());
    final ProjectOperator projectOperator = new ProjectOperator(
        logger,
        logicalNode,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final QueryRowImpl windowedRow = QueryRowImpl.of(
        WINDOWED_INTERMEDIATE_SCHEMA_WITH_PSEUDO,
        A_KEY,
        Optional.of(A_WINDOW),
        GenericRow.genericRow("a", "b", A_ROWTIME, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(
        A_KEY,
        windowedRow.value()
    ))
        .thenReturn(GenericRow.genericRow("k", "b"));
    projectOperator.open();

    // When:
    QueryRow result = (QueryRow) projectOperator.next();

    // Then:
    final List<Object> expected = new ArrayList<>(windowedRow.key().values());
    expected.add(windowedRow.value().values().get(1));
    assertThat(result.value().values(), is(expected));
  }
}
