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

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWEND_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWSTART_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceNodeTest {

  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");
  private static final SourceName SOURCE_NAME = SourceName.of("datasource");

  private final KsqlConfig realConfig = new KsqlConfig(Collections.emptyMap());
  private SchemaKStream<?> realStream;
  private StreamsBuilder realBuilder;

  private static final ColumnName K0 = ColumnName.of("k0");
  private static final ColumnName K1 = ColumnName.of("k1");
  private static final ColumnName FIELD1 = ColumnName.of("field1");
  private static final ColumnName FIELD2 = ColumnName.of("field2");
  private static final ColumnName FIELD3 = ColumnName.of("field3");
  private static final ColumnName TIMESTAMP_FIELD = ColumnName.of("timestamp");
  private static final ColumnName KEY = ColumnName.of("key");

  private static final LogicalSchema REAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.INTEGER)
      .keyColumn(K1, SqlTypes.INTEGER)
      .valueColumn(FIELD1, SqlTypes.INTEGER)
      .valueColumn(FIELD2, SqlTypes.STRING)
      .valueColumn(FIELD3, SqlTypes.STRING)
      .valueColumn(TIMESTAMP_FIELD, SqlTypes.BIGINT)
      .valueColumn(KEY, SqlTypes.STRING)
      .build();

  private static final TimestampColumn TIMESTAMP_COLUMN =
      new TimestampColumn(TIMESTAMP_FIELD, Optional.empty());

  private final KsqlStream<String> SOME_SOURCE = new KsqlStream<>(
      "sqlExpression",
      SOURCE_NAME,
      REAL_SCHEMA,
      Optional.of(
          new TimestampColumn(
              ColumnName.of("timestamp"),
              Optional.empty()
          )
      ),
        false,
      new KsqlTopic(
          "topic",
          KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
          ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
      ),
      false
  );

  @Mock
  private DataSource dataSource;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private Serde<String> keySerde;
  @Mock
  private PlanBuildContext buildContext;
  @Mock
  private RuntimeBuildContext executeContext;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private DataSourceNode.SchemaKStreamFactory schemaKStreamFactory;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;
  @Mock
  private SchemaKStream<Struct> stream;
  @Mock
  private SchemaKTable<Struct> table;
  @Mock
  private KsqlTopic topic;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private Projection projection;

  private DataSourceNode node;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    realBuilder = new StreamsBuilder();

    when(buildContext.getKsqlConfig()).thenReturn(realConfig);
    when(buildContext.getFunctionRegistry()).thenReturn(functionRegistry);
    when(buildContext.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    when(executeContext.getKsqlConfig()).thenReturn(realConfig);
    when(executeContext.getStreamsBuilder()).thenReturn(realBuilder);
    when(executeContext.getProcessingLogger(any())).thenReturn(processingLogger);
    when(executeContext.buildKeySerde(any(), any(), any()))
        .thenReturn((Serde)keySerde);
    when(executeContext.buildValueSerde(any(), any(), any())).thenReturn(rowSerde);

//    This stubbing is unnecessary so commenting it out for now
//    when(rowSerde.serializer()).thenReturn(mock(Serializer.class));
    when(rowSerde.deserializer()).thenReturn(mock(Deserializer.class));

    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    when(schemaKStreamFactory.create(any(), any(), any()))
        .thenAnswer(inv -> inv.<DataSource>getArgument(1)
            .getDataSourceType() == DataSourceType.KSTREAM
            ? stream : table
        );

    givenWindowedSource(false);

    node = new DataSourceNode(
        PLAN_NODE_ID,
        SOME_SOURCE,
        SOME_SOURCE.getName(),
        false
    );
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    realStream = buildStream(node);

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(realBuilder.build(), PlanTestUtil.SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(PlanTestUtil.TRANSFORM_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("topic")));
  }

  @Test
  public void shouldBuildTransformNode() {
    // When:
    realStream = buildStream(node);

    // Then:
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        realBuilder.build(), PlanTestUtil.TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(PlanTestUtil.SOURCE_NODE), Collections.emptyList());
  }

  @Test
  public void shouldBeOfTypeSchemaKStreamWhenDataSourceIsKsqlStream() {
    // When:
    realStream = buildStream(node);

    // Then:
    assertThat(realStream.getClass(), equalTo(SchemaKStream.class));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    // Given:
    final KsqlTable<String> table = new KsqlTable<>("sqlExpression",
        SourceName.of("datasource"),
        REAL_SCHEMA,
        Optional.of(TIMESTAMP_COLUMN),
        false,
        new KsqlTopic(
            "topic2",
            KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
            ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
        ),
        false
    );

    node = new DataSourceNode(
        PLAN_NODE_ID,
        table,
        table.getName(),
        false
    );

    // When:
    final SchemaKStream<?> result = buildStream(node);

    // Then:
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldHaveFullyQualifiedSchema() {
    // When:
    final LogicalSchema schema = node.getSchema();

    // Then:
    assertThat(schema, is(REAL_SCHEMA.withPseudoAndKeyColsInValue(false)));
  }

  @Test
  public void shouldHaveFullyQualifiedWindowedSchema() {
    // Given:
    givenWindowedSource(true);
    givenNodeWithMockSource();

    // When:
    final LogicalSchema schema = node.getSchema();

    // Then:
    assertThat(schema, is(REAL_SCHEMA.withPseudoAndKeyColsInValue(true)));
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndex() {
    // Given:
    givenNodeWithMockSource();

    // When:
    node.buildStream(buildContext);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any());
  }

  // should this even be possible? if you are using a timestamp extractor then shouldn't the name
  // should be unqualified
  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndexForQualifiedFieldName() {
    // Given:
    givenNodeWithMockSource();

    // When:
    node.buildStream(buildContext);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any());
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParams() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    givenNodeWithMockSource();

    // When:
    final SchemaKStream<?> returned = node.buildStream(buildContext);

    // Then:
    assertThat(returned, is(stream));
    verify(schemaKStreamFactory).create(
        same(buildContext),
        same(dataSource),
        stackerCaptor.capture()
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "Source"))
    );
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParamsWhenBuildingTable() {
    // Given:
    givenNodeWithMockSource();

    // When:
    node.buildStream(buildContext);

    // Then:
    verify(schemaKStreamFactory).create(
        same(buildContext),
        same(dataSource),
        stackerCaptor.capture()
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "Source"))
    );
  }

  @Test
  public void shouldBuildTableByConvertingFromStream() {
    // Given:
    givenNodeWithMockSource();

    // When:
    final SchemaKStream<?> returned = node.buildStream(buildContext);

    // Then:
    assertThat(returned, is(table));
  }

  @Test
  public void shouldThrowOnResolveSelectStarIfWrongSourceName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> node.resolveSelectStar(Optional.of(SourceName.of("wrong")))
    );
  }

  @Test
  public void shouldNotThrowOnResolveSelectStarIfRightSourceName() {
    node.resolveSelectStar(Optional.of(SOME_SOURCE.getName()));
  }

  @Test
  public void shouldResolveSelectStartToAllColumns() {
    // When:
    final Stream<ColumnName> result = node.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(K0, K1, FIELD1, FIELD2, FIELD3, TIMESTAMP_FIELD, KEY));
  }

  @Test
  public void shouldResolveSelectStartToAllColumnsIncludingWindowBounds() {
    // Given:
    givenWindowedSource(true);
    givenNodeWithMockSource();

    // When:
    final Stream<ColumnName> result = node.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(
        columns,
        contains(K0, K1, WINDOWSTART_NAME, WINDOWEND_NAME, FIELD1, FIELD2, FIELD3, TIMESTAMP_FIELD, KEY)
    );
  }

  @Test
  public void shouldNotThrowIfProjectionContainsKeyColumns() {
    // Given:
    when(projection.containsExpression(any())).thenReturn(true);

    // When:
    node.validateKeyPresent(SOURCE_NAME, projection);

    // Then: did not throw.
  }

  @Test
  public void shouldThrowIfProjectionDoesNotContainKeyColumns() {
    // Given:
    when(projection.containsExpression(any())).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> node.validateKeyPresent(SOURCE_NAME, projection)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The query used to build `datasource` must include "
        + "the key columns k0 and k1 in its projection (eg, SELECT k0, k1...)."));
  }

  @Test
  public void shouldThrowIfProjectionDoesNotContainAllKeyColumns() {
    // Given:
    when(projection.containsExpression(new UnqualifiedColumnReferenceExp(K0))).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> node.validateKeyPresent(SOURCE_NAME, projection)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The query used to build `datasource` must include " +
        "the key columns k0 and k1 in its projection (eg, SELECT k0, k1...)."));
  }

  private void givenNodeWithMockSource() {
    when(dataSource.getSchema()).thenReturn(REAL_SCHEMA);
    node = new DataSourceNode(
        PLAN_NODE_ID,
        dataSource,
        SOURCE_NAME,
        schemaKStreamFactory,
        false
    );
  }

  private SchemaKStream<?> buildStream(final DataSourceNode node) {
    final SchemaKStream<?> stream = node.buildStream(buildContext);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable<?> table = (SchemaKTable<?>) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(executeContext));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(executeContext));
    }
    return stream;
  }

  private void givenWindowedSource(final boolean windowed) {
    final FormatInfo format = FormatInfo.of(FormatFactory.KAFKA.name());

    final KeyFormat keyFormat = windowed
        ? KeyFormat
        .windowed(format, SerdeFeatures.of(), WindowInfo.of(WindowType.SESSION, Optional.empty(), Optional.empty()))
        : KeyFormat.nonWindowed(format, SerdeFeatures.of());

    when(topic.getKeyFormat()).thenReturn(keyFormat);
  }
}
