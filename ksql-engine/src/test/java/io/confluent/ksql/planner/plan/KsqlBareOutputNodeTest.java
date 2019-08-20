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

import static io.confluent.ksql.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.QueryIdGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlBareOutputNodeTest {

  private static final String SOURCE_NODE = "KSTREAM-SOURCE-0000000000";
  private static final String SOURCE_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000001";
  private static final String TRANSFORM_NODE = "KSTREAM-TRANSFORMVALUES-0000000002";
  private static final String FILTER_NODE = "KSTREAM-FILTER-0000000003";
  private static final String FILTER_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000004";
  private static final String SIMPLE_SELECT_WITH_FILTER = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";

  private SchemaKStream stream;
  private StreamsBuilder builder;
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryId queryId = new QueryId("output-test");
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());

  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KeySerde<Struct> keySerde;

  @SuppressWarnings("unchecked")
  @Before
  public void before() {
    builder = new StreamsBuilder();

    when(ksqlStreamBuilder.getKsqlConfig()).thenReturn(new KsqlConfig(Collections.emptyMap()));
    when(ksqlStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(ProcessingLogContext.create());
    when(ksqlStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(ksqlStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker(queryId)
            .push(inv.getArgument(0).toString()));
    when(ksqlStreamBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);

    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) AnalysisTestUtil
        .buildLogicalPlan(ksqlConfig, SIMPLE_SELECT_WITH_FILTER, metaStore);

    stream = planNode.buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldBuildSourceNode() {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(SOURCE_MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test1")));
  }

  @Test
  public void shouldBuildMapNode() {
    verifyProcessorNode((TopologyDescription.Processor) getNodeByName(SOURCE_MAPVALUES_NODE),
        Collections.singletonList(SOURCE_NODE),
        Collections.singletonList(TRANSFORM_NODE));
  }

  @Test
  public void shouldBuildTransformNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(SOURCE_MAPVALUES_NODE), Collections.singletonList(FILTER_NODE));
  }

  @Test
  public void shouldBuildFilterNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(FILTER_NODE);
    verifyProcessorNode(node, Collections.singletonList(TRANSFORM_NODE), Collections.singletonList(FILTER_MAPVALUES_NODE));
  }

  @Test
  public void shouldCreateCorrectSchema() {
    final LogicalSchema schema = stream.getSchema();
    assertThat(schema.valueFields(), contains(
        Field.of("COL0", SqlTypes.BIGINT),
        Field.of("COL2", SqlTypes.STRING),
        Field.of("COL3", SqlTypes.DOUBLE)));
  }

  @Test
  public void shouldComputeQueryIdCorrectly() {
    // Given:
    final KsqlBareOutputNode node
        = (KsqlBareOutputNode) AnalysisTestUtil
        .buildLogicalPlan(ksqlConfig, "select col0 from test1;", metaStore);
    final QueryIdGenerator queryIdGenerator = mock(QueryIdGenerator.class);

    // When:
    final Set<QueryId> ids = IntStream.range(0, 100)
        .mapToObj(i -> node.getQueryId(queryIdGenerator))
        .collect(Collectors.toSet());

    // Then:
    assertThat(ids.size(), equalTo(100));
    verifyNoMoreInteractions(queryIdGenerator);
  }

  private TopologyDescription.Node getNodeByName(final String nodeName) {
    return PlanTestUtil.getNodeByName(builder.build(), nodeName);
  }
}
