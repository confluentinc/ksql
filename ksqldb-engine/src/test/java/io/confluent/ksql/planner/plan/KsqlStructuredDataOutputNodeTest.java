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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlStructuredDataOutputNodeTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("timestamp"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .build();

  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");
  private static final KeyFormat PROTOBUF_KEY_FORMAT = KeyFormat
      .nonWindowed(FormatInfo.of(FormatFactory.PROTOBUF.name()), SerdeFeatures.of());
  private static final ValueFormat JSON_FORMAT = ValueFormat
      .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of());

  @Mock
  private PlanBuildContext planBuildContext;
  @Mock
  private FinalProjectNode sourceNode;
  @Mock
  private SchemaKStream<String> sourceStream;
  @Mock
  private SchemaKStream<?> sinkStream;
  @Mock
  private KsqlTopic ksqlTopic;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;

  private KsqlStructuredDataOutputNode outputNode;
  private boolean createInto;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void before() {
    createInto = true;

    when(sourceNode.getSchema()).thenReturn(LogicalSchema.builder().build());
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(sourceNode.buildStream(planBuildContext)).thenReturn((SchemaKStream) sourceStream);

    when(sourceStream.into(any(), any(), any()))
        .thenReturn((SchemaKStream) sinkStream);

    when(planBuildContext.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    buildNode();
  }

  @Test
  public void shouldThrowIfSelectExpressionsHaveSameNameAsAnyKeyColumn() {
    // Given:
    givenSourceSchema(
        LogicalSchema.builder()
            .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
            .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("k0"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
            .build()
    );

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        this::buildNode
    );

    // Then:
    assertThat(e.getMessage(), containsString("Value columns clash with key columns: `k0`"));
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    outputNode.buildStream(planBuildContext);

    // Then:
    verify(sourceNode).buildStream(planBuildContext);
  }

  @Test
  public void shouldBuildOutputNodeForInsertIntoAvroFromNonAvro() {
    // Given:
    givenInsertIntoNode();

    KeyFormat.nonWindowed(
        FormatInfo.of(
            FormatFactory.AVRO.name(),
            ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "key-name")
        ),
        SerdeFeatures.of()
    );

    ValueFormat.of(
        FormatInfo.of(
            FormatFactory.AVRO.name(),
            ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "name")
        ),
        SerdeFeatures.of()
    );

    // When:
    outputNode.buildStream(planBuildContext);

    // Then:
    verify(sourceStream).into(eq(ksqlTopic), any(), any());
  }

  @Test
  public void shouldCallInto() {
    // When:
    final SchemaKStream<?> result = outputNode.buildStream(planBuildContext);

    // Then:
    verify(sourceStream).into(
        eq(ksqlTopic),
        stackerCaptor.capture(),
        eq(outputNode.getTimestampColumn())
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0"))
    );
    assertThat(result, sameInstance(sinkStream));
  }

  @Test
  public void shouldSetReplaceFlagToTrue() {
    // When
    final KsqlStructuredDataOutputNode outputNode = new KsqlStructuredDataOutputNode(
        PLAN_NODE_ID,
        sourceNode,
        SCHEMA,
        Optional.empty(),
        ksqlTopic,
        OptionalInt.empty(),
        createInto,
        SourceName.of(PLAN_NODE_ID.toString()),
        true);

    // Then
    assertThat(outputNode.getOrReplace(), is(true));
  }

  @Test
  public void shouldSetReplaceFlagToFalse() {
    // When
    final KsqlStructuredDataOutputNode outputNode = new KsqlStructuredDataOutputNode(
        PLAN_NODE_ID,
        sourceNode,
        SCHEMA,
        Optional.empty(),
        ksqlTopic,
        OptionalInt.empty(),
        createInto,
        SourceName.of(PLAN_NODE_ID.toString()),
        false);

    // Then
    assertThat(outputNode.getOrReplace(), is(false));
  }

  private void givenInsertIntoNode() {
    this.createInto = false;
    buildNode();
  }

  private void buildNode() {
    outputNode = new KsqlStructuredDataOutputNode(
        PLAN_NODE_ID,
        sourceNode,
        SCHEMA,
        Optional.empty(),
        ksqlTopic,
        OptionalInt.empty(),
        createInto,
        SourceName.of(PLAN_NODE_ID.toString()),
        false
    );
  }

  private void givenSourceSchema(final LogicalSchema schema) {
    when(sourceNode.getSchema())
        .thenReturn(schema);
  }
}
