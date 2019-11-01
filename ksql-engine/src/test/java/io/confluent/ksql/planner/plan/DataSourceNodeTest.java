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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.streams.KSPlanBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
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

  private static final ColumnRef TIMESTAMP_FIELD
      = ColumnRef.withoutSource(ColumnName.of("timestamp"));
  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");

  private final KsqlConfig realConfig = new KsqlConfig(Collections.emptyMap());
  private SchemaKStream realStream;
  private StreamsBuilder realBuilder;

  private static final ColumnName FIELD1 = ColumnName.of("field1");
  private static final ColumnName FIELD2 = ColumnName.of("field2");
  private static final ColumnName FIELD3 = ColumnName.of("field3");

  private static final LogicalSchema REAL_SCHEMA = LogicalSchema.builder()
      .valueColumn(FIELD1, SqlTypes.STRING)
      .valueColumn(FIELD2, SqlTypes.STRING)
      .valueColumn(FIELD3, SqlTypes.STRING)
      .valueColumn(TIMESTAMP_FIELD.name(), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .build();
  private static final KeyField KEY_FIELD
      = KeyField.of(ColumnRef.withoutSource(ColumnName.of("field1")));

  private static final Optional<AutoOffsetReset> OFFSET_RESET = Optional.of(AutoOffsetReset.LATEST);

  private final KsqlStream<String> SOME_SOURCE = new KsqlStream<>(
      "sqlExpression",
      SourceName.of("datasource"),
      REAL_SCHEMA,
      SerdeOption.none(),
      KeyField.of(ColumnRef.withoutSource(ColumnName.of("key"))),
      new LongColumnTimestampExtractionPolicy(ColumnRef.withoutSource(ColumnName.of("timestamp"))),
      new KsqlTopic(
          "topic",
          KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
          ValueFormat.of(FormatInfo.of(Format.JSON)),
          false
      )
  );

  private final DataSourceNode node = new DataSourceNode(
      PLAN_NODE_ID,
      SOME_SOURCE,
      SOME_SOURCE.getName(),
      Collections.emptyList()
  );

  @Mock
  private DataSource<?> dataSource;
  @Mock
  private TimestampExtractionPolicy timestampExtractionPolicy;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private KeySerde<String> keySerde;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private DataSourceNode.SchemaKStreamFactory schemaKStreamFactory;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;
  @Mock
  private SchemaKStream stream;
  @Mock
  private SchemaKTable table;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    realBuilder = new StreamsBuilder();

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(realConfig);
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(realBuilder);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    when(ksqlStreamBuilder.buildKeySerde(any(), any(), any()))
        .thenReturn((KeySerde)keySerde);
    when(ksqlStreamBuilder.buildValueSerde(any(), any(), any())).thenReturn(rowSerde);
    when(ksqlStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);

    when(rowSerde.serializer()).thenReturn(mock(Serializer.class));
    when(rowSerde.deserializer()).thenReturn(mock(Deserializer.class));

    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(dataSource.getTimestampExtractionPolicy()).thenReturn(timestampExtractionPolicy);
    when(ksqlTopic.getKeyFormat()).thenReturn(KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)));
    when(ksqlTopic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.JSON)));
    when(timestampExtractionPolicy.getTimestampField()).thenReturn(TIMESTAMP_FIELD);
    when(schemaKStreamFactory.create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(stream);
    when(stream.toTable(any(), any(), any())).thenReturn(table);
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
  public void shouldBuildStreamWithSameKeyField() {
    // When:
    final SchemaKStream<?> stream = buildStream(node);

    // Then:
    assertThat(stream.getKeyField(), is(node.getKeyField()));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    final KsqlTable<String> table = new KsqlTable<>("sqlExpression",
        SourceName.of("datasource"),
        REAL_SCHEMA,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("field1"))),
        new LongColumnTimestampExtractionPolicy(TIMESTAMP_FIELD),
        new KsqlTopic(
            "topic2",
            KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
            ValueFormat.of(FormatInfo.of(Format.JSON)),
            false
        )
    );

    final DataSourceNode node = new DataSourceNode(
        PLAN_NODE_ID,
        table,
        table.getName(),
        Collections.emptyList());

    final SchemaKStream result = buildStream(node);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldHaveFullyQualifiedSchema() {
    // Given:
    final SourceName sourceName = SOME_SOURCE.getName();

    // When:
    final LogicalSchema schema = node.getSchema();

    // Then:
    assertThat(schema, is(
        LogicalSchema.builder()
            .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
            .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
            .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
            .valueColumn(TIMESTAMP_FIELD.name(), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
            .build().withAlias(sourceName)));
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndex() {
    // Given:
    reset(timestampExtractionPolicy);
    when(timestampExtractionPolicy.getTimestampField()).thenReturn(ColumnRef.withoutSource(FIELD2));
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any(), any(), eq(1), any(), any(), any());
  }

  // should this even be possible? if you are using a timestamp extractor then shouldn't the name
  // should be unqualified
  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndexForQualifiedFieldName() {
    // Given:
    reset(timestampExtractionPolicy);
    when(timestampExtractionPolicy.getTimestampField())
        .thenReturn(ColumnRef.withoutSource(ColumnName.of("field2")));
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any(), any(), eq(1), any(), any(), any());
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParams() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    final SchemaKStream returned = node.buildStream(ksqlStreamBuilder);

    // Then:
    assertThat(returned, is(stream));
    verify(schemaKStreamFactory).create(
        same(ksqlStreamBuilder),
        same(dataSource),
        eq(StreamSource.getSchemaWithMetaAndKeyFields(SourceName.of("name"), REAL_SCHEMA)),
        stackerCaptor.capture(),
        eq(3),
        eq(OFFSET_RESET),
        same(node.getKeyField()),
        eq(SourceName.of("name"))
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "source"))
    );
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParamsWhenBuildingTable() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(
        same(ksqlStreamBuilder),
        same(dataSource),
        eq(StreamSource.getSchemaWithMetaAndKeyFields(SourceName.of("name"), REAL_SCHEMA)),
        stackerCaptor.capture(),
        eq(3),
        eq(OFFSET_RESET),
        same(node.getKeyField()),
        eq(SourceName.of("name"))
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "source"))
    );
  }

  @Test
  public void shouldBuildTableByConvertingFromStream() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    final SchemaKStream returned = node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(stream).toTable(any(), any(), any());
    assertThat(returned, is(table));
  }

  @Test
  public void shouldBuildTableWithCorrectContext() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(stream).toTable(any(), any(), stackerCaptor.capture());
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "reduce")));
  }

  private DataSourceNode buildNodeWithMockSource() {
    when(dataSource.getSchema()).thenReturn(REAL_SCHEMA);
    when(dataSource.getKeyField()).thenReturn(KEY_FIELD);
    return new DataSourceNode(
        PLAN_NODE_ID,
        dataSource,
        SourceName.of("name"),
        Collections.emptyList(),
        schemaKStreamFactory
    );
  }

  private SchemaKStream buildStream(final DataSourceNode node) {
    final SchemaKStream stream = node.buildStream(ksqlStreamBuilder);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable table = (SchemaKTable) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(ksqlStreamBuilder));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(ksqlStreamBuilder));
    }
    return stream;
  }
}
