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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryIdGenerator;
import io.confluent.ksql.util.timestamp.LongColumnTimestampExtractionPolicy;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class KsqlStructuredDataOutputNodeTest {

  private static final String QUERY_ID_STRING = "output-test";
  private static final QueryId QUERY_ID = new QueryId(QUERY_ID_STRING);

  private static final String SINK_KAFKA_TOPIC_NAME = "output_kafka";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueField("field1", SqlTypes.STRING)
      .valueField("field2", SqlTypes.STRING)
      .valueField("field3", SqlTypes.STRING)
      .valueField("timestamp", SqlTypes.BIGINT)
      .valueField("key", SqlTypes.STRING)
      .build();

  private static final KeyField KEY_FIELD =
      KeyField.of("key", SCHEMA.findValueField("key").get());
  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");
  private static final ValueFormat JSON_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private QueryIdGenerator queryIdGenerator;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream<String> sourceStream;
  @Mock
  private SchemaKStream<String> resultStream;
  @Mock
  private SchemaKStream<?> sinkStream;
  @Mock
  private SchemaKStream<?> resultWithKeySelected;
  @Mock
  private SchemaKStream<?> sinkStreamWithKeySelected;
  @Mock
  private KStream<String, GenericRow> kstream;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Captor
  private ArgumentCaptor<QueryContext> queryContextCaptor;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;

  private final Set<SerdeOption> serdeOptions = SerdeOption.none();

  private KsqlStructuredDataOutputNode outputNode;
  private LogicalSchema schema;
  private Optional<String> partitionBy;
  private boolean createInto;

  @SuppressWarnings("unchecked")
  @Before
  public void before() {
    schema = SCHEMA;
    partitionBy = Optional.empty();
    createInto = true;

    when(queryIdGenerator.getNextId()).thenReturn(QUERY_ID_STRING);

    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(sourceNode.buildStream(ksqlStreamBuilder)).thenReturn((SchemaKStream) sourceStream);

    when(sourceStream.getKeyField()).thenReturn(KeyField.none());

    when(sourceStream.withKeyField(any()))
        .thenReturn(resultStream);
    when(resultStream.into(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn((SchemaKStream) sinkStream);
    when(resultStream.selectKey(any(), anyBoolean(), any()))
        .thenReturn((SchemaKStream) resultWithKeySelected);
    when(resultWithKeySelected.into(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn((SchemaKStream) sinkStreamWithKeySelected);

    when(ksqlStreamBuilder.buildValueSerde(any(), any(), any())).thenReturn(rowSerde);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(QUERY_ID)
            .push(inv.getArgument(0).toString()));
    when(ksqlTopic.getKafkaTopicName()).thenReturn(SINK_KAFKA_TOPIC_NAME);
    when(ksqlTopic.getValueFormat()).thenReturn(JSON_FORMAT);

    buildNode();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfPartitionByAndKeyFieldNone() {
    // When:
    new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        SCHEMA,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        KeyField.none(),
        ksqlTopic,
        Optional.of("something"),
        OptionalInt.empty(),
        false,
        SerdeOption.none()
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfPartitionByDoesNotMatchKeyField() {
    // When:
    new KsqlStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        SCHEMA,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        KeyField.of(Optional.of("something else"), Optional.empty()),
        ksqlTopic,
        Optional.of("something"),
        OptionalInt.empty(),
        false,
        SerdeOption.none()
    );
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(sourceNode).buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldBuildMapNodePriorToOutput() {
    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    final InOrder inOrder = Mockito.inOrder(sourceNode, sourceStream);

    inOrder.verify(sourceNode)
        .buildStream(any());

    inOrder.verify(sourceStream).withKeyField(any());
  }

  @Test
  public void shouldBuildOutputNode() {
    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(sourceStream).withKeyField(
        KEY_FIELD.withName(Optional.empty())
    );
  }

  @Test
  public void shouldPartitionByFieldNameInPartitionByProperty() {
    // Given:
    givenNodePartitioningByKey("key");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).selectKey(
        KEY_FIELD.name().get(),
        false,
        new QueryContext.Stacker(QUERY_ID).push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldPartitionByRowKey() {
    // Given:
    givenNodePartitioningByKey("ROWKEY");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).selectKey(
        "ROWKEY",
        false,
        new QueryContext.Stacker(QUERY_ID).push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldPartitionByRowTime() {
    // Given:
    givenNodePartitioningByKey("ROWTIME");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).selectKey(
        "ROWTIME",
        false,
        new QueryContext.Stacker(QUERY_ID).push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForStream() {
    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("CSAS_0_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForTable() {
    // Given:
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    givenNodeWithSchema(SCHEMA);

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("CTAS_0_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForInsertInto() {
    // Given:
    givenInsertIntoNode();

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNextId();
    assertThat(queryId, equalTo(new QueryId("InsertQuery_" + QUERY_ID_STRING)));
  }

  @Test
  public void shouldBuildOutputNodeForInsertIntoAvroFromNonAvro() {
    // Given:
    givenInsertIntoNode();

    final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("name")));

    when(ksqlTopic.getValueFormat()).thenReturn(valueFormat);

    // When/Then (should not throw):
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(ksqlStreamBuilder).buildValueSerde(
        eq(valueFormat.getFormatInfo()),
        any(),
        any()
    );
  }

  @Test
  public void shouldBuildRowSerdeCorrectly() {
    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(ksqlStreamBuilder).buildValueSerde(
        eq(FormatInfo.of(Format.JSON)),
        eq(PhysicalSchema.from(SCHEMA, serdeOptions)),
        queryContextCaptor.capture()
    );

    assertThat(QueryLoggerUtil.queryLoggerName(queryContextCaptor.getValue()),
        is("output-test.0"));
  }

  @Test
  public void shouldCallInto() {
    // When:
    final SchemaKStream result = outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).into(
        eq(SINK_KAFKA_TOPIC_NAME),
        same(rowSerde),
        eq(SCHEMA),
        eq(JSON_FORMAT),
        eq(SerdeOption.none()),
        eq(ImmutableSet.of()),
        stackerCaptor.capture()
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0"))
    );
    assertThat(result, sameInstance(sinkStream));
  }

  @Test
  public void shouldCallIntoWithIndexesToRemoveImplicitsAndRowKey() {
    // Given:
    final LogicalSchema schema = SCHEMA.withMetaAndKeyFieldsInValue();
    givenNodeWithSchema(schema);

    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).into(
        eq(SINK_KAFKA_TOPIC_NAME),
        same(rowSerde),
        any(),
        any(),
        any(),
        eq(ImmutableSet.of(0, 1)),
        any()
    );
  }

  @Test
  public void shouldCallIntoWithIndexesToRemoveImplicitsAndRowKeyRegardlessOfLocation() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField("field1", SqlTypes.STRING)
        .valueField("field2", SqlTypes.STRING)
        .valueField("ROWKEY", SqlTypes.STRING)
        .valueField("field3", SqlTypes.STRING)
        .valueField("timestamp", SqlTypes.BIGINT)
        .valueField("ROWTIME", SqlTypes.BIGINT)
        .valueField("key", SqlTypes.STRING)
        .build();

    givenNodeWithSchema(schema);

    // When:
    outputNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(resultStream).into(
        eq(SINK_KAFKA_TOPIC_NAME),
        same(rowSerde),
        any(),
        any(),
        any(),
        eq(ImmutableSet.of(2, 5)),
        any()
    );
  }

  private void givenInsertIntoNode() {
    this.createInto = false;
    buildNode();
  }

  private void givenNodePartitioningByKey(final String field) {
    this.partitionBy = Optional.of(field);
    buildNode();
  }

  private void givenNodeWithSchema(final LogicalSchema schema) {
    this.schema = schema;
    buildNode();
  }

  private void buildNode() {
    outputNode = new KsqlStructuredDataOutputNode(
        PLAN_NODE_ID,
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy("timestamp"),
        KEY_FIELD,
        ksqlTopic,
        partitionBy,
        OptionalInt.empty(),
        createInto,
        SerdeOption.none()
    );
  }
}