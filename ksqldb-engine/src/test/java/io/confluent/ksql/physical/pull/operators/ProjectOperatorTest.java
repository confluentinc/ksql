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

package io.confluent.ksql.physical.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.Window;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory.SelectValueMapperFactorySupplier;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProjectOperatorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();

  private static final Struct A_KEY = StructKeyUtil
      .keyBuilder(ColumnName.of("k0"), SqlTypes.STRING).build("k", 0);
  private static final long A_ROWTIME = 12335L;

  private static final Window A_WINDOW = Window.of(Instant.now(), Instant.now().plusMillis(10));
  private static final TimeWindow STREAM_WINDOW = new TimeWindow(
      A_WINDOW.start().toEpochMilli(),
      A_WINDOW.end().toEpochMilli()
  );

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private MetaStore metaStore;
  @Mock
  private ProcessingLogger logger;
  @Mock
  private Materialization mat;
  @Mock
  private LogicalSchema outputSchema;
  @Mock
  private SelectValueMapperFactorySupplier selectValueMapperFactorySupplier;
  @Mock
  private ProjectNode logicalNode;
  @Mock
  private AbstractPhysicalOperator child;
  @Mock
  private KsqlTransformer<Object, GenericRow> transformer;
  @Mock
  private SelectValueMapper<Object> selectValueMapper;

  @Test
  public void shouldProjectAllColumnsWhenSelectStarNonWindowed() {
    // Given:
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        outputSchema,
        true,
        false,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final Row row = Row.of(
        SCHEMA,
        A_KEY,
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    projectOperator.open();

    // Then:
    final List<Object> rowList = new ArrayList<>();
    row.key().schema().fields().stream().map(row.key()::get).forEach(rowList::add);
    rowList.addAll(row.value().values());
    assertThat(projectOperator.next(), is(rowList));
  }

  @Test
  public void shouldProjectAllColumnsWhenSelectStarWindowed() {
    // Given:
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        outputSchema,
        true,
        true,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final WindowedRow windowedRow = WindowedRow.of(
        SCHEMA,
        new Windowed<>(A_KEY, STREAM_WINDOW),
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    projectOperator.open();

    // Then:
    final List<Object> rowList = new ArrayList<>();
    windowedRow.key().schema().fields().stream().map(windowedRow.key()::get).forEach(rowList::add);
    rowList.add(windowedRow.window().get().start().toEpochMilli());
    rowList.add(windowedRow.window().get().end().toEpochMilli());
    rowList.addAll(windowedRow.value().values());
    assertThat(projectOperator.next(), is(rowList));
  }

  @Test
  public void shouldCallTransformWithCorrectArguments() {
    // Given:
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        SCHEMA,
        false,
        false,
        true,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final Row row = Row.of(
        SCHEMA,
        A_KEY,
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any(), any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(any(), any(), any())).thenReturn(GenericRow.genericRow("k", "a", "b"));
    when(mat.schema()).thenReturn(SCHEMA);
    projectOperator.open();
    projectOperator.next();

    // Then:
    verify(transformer).transform(
        A_KEY, GenericRow.genericRow("a", "b", 12335L, "k"), new PullProcessingContext(12335L));
  }

  @Test
  public void shouldCallTransformWithCorrectArgumentsWindowed() {
    // Given:
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        SCHEMA,
        false,
        false,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final WindowedRow windowedRow = WindowedRow.of(
        SCHEMA,
        new Windowed<>(A_KEY, STREAM_WINDOW),
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any(), any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(any(), any(), any())).thenReturn(GenericRow.genericRow("k", "a", "b"));
    when(mat.schema()).thenReturn(SCHEMA);
    projectOperator.open();
    projectOperator.next();

    // Then:
    verify(transformer).transform(
        A_KEY,
        GenericRow.genericRow("a", "b", 12335L, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        new PullProcessingContext(12335L));
  }

  @Test
  public void shouldProjectOnlyKeyNonWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .build();
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        schema,
        false,
        false,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final Row row = Row.of(
        SCHEMA,
        A_KEY,
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any(), any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, GenericRow.genericRow("a", "b", 12335L, "k"), new PullProcessingContext(12335L)))
             .thenAnswer(inv -> GenericRow.genericRow("k"));
    when(mat.schema()).thenReturn(SCHEMA);
    projectOperator.open();

    // Then:
    final List<Object> rowList = new ArrayList<>();
    row.key().schema().fields().stream().map(row.key()::get).forEach(rowList::add);
    assertThat(projectOperator.next(), is(rowList));
  }

  @Test
  public void shouldProjectKeyAndValueNonWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .build();
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        schema,
        false,
        false,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    final Row row = Row.of(
        SCHEMA,
        A_KEY,
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(row);
    when(selectValueMapperFactorySupplier.create(any(), any(), any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(A_KEY, GenericRow.genericRow("a", "b", 12335L, "k"), new PullProcessingContext(12335L)))
        .thenAnswer(inv -> GenericRow.genericRow("k","b"));
    when(mat.schema()).thenReturn(SCHEMA);
    projectOperator.open();

    // Then:
    final List<Object> rowList = new ArrayList<>();
    row.key().schema().fields().stream().map(row.key()::get).forEach(rowList::add);
    rowList.add(row.value().values().get(1));
    assertThat(projectOperator.next(), is(rowList));
  }

  @Test
  public void shouldProjectKeyAndValueWindowed() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
        .build();
    final ProjectOperator projectOperator = new ProjectOperator(
        ksqlConfig,
        metaStore,
        logger,
        mat,
        logicalNode,
        schema,
        false,
        false,
        false,
        selectValueMapperFactorySupplier);
    projectOperator.addChild(child);
    when(mat.windowType()).thenReturn(Optional.of(WindowType.TUMBLING));
    final WindowedRow windowedRow = WindowedRow.of(
        SCHEMA,
        new Windowed<>(A_KEY, STREAM_WINDOW),
        GenericRow.genericRow("a", "b"),
        A_ROWTIME
    );
    when(child.next()).thenReturn(windowedRow);
    when(selectValueMapperFactorySupplier.create(any(), any(), any(), any()))
        .thenReturn(selectValueMapper);
    when(selectValueMapper.getTransformer(logger)).thenReturn(transformer);
    when(transformer.transform(
        A_KEY,
        GenericRow.genericRow("a", "b", 12335L, "k", A_WINDOW.start().toEpochMilli(), A_WINDOW.end().toEpochMilli()),
        new PullProcessingContext(12335L)))
        .thenAnswer(inv -> GenericRow.genericRow("k", "b"));
    when(mat.schema()).thenReturn(SCHEMA);
    projectOperator.open();

    // Then:
    final List<Object> rowList = new ArrayList<>();
    windowedRow.key().schema().fields().stream().map(windowedRow.key()::get).forEach(rowList::add);
    rowList.add(windowedRow.value().values().get(1));
    assertThat(projectOperator.next(), is(rowList));
  }

}
