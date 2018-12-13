/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;

import static io.confluent.ksql.planner.plan.PlanTestUtil.MAPVALUES_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.SOURCE_NODE;
import static io.confluent.ksql.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateNodeTest {

  private static final FunctionRegistry functionRegistry = new InternalFunctionRegistry();

  static {
    UdfLoaderUtil.load(functionRegistry);
  }

  @Mock
  private ServiceContext serviceContext;
  private final KsqlConfig ksqlConfig =  new KsqlConfig(new HashMap<>());
  private final StreamsBuilder builder = new StreamsBuilder();

  @Test
  public void shouldBuildSourceNode() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(MAPVALUES_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test1")));
  }

  @Test
  public void shouldHaveOneSubTopologyIfGroupByKey() {
    // When:
    buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(1));
  }

  @Test
  public void shouldHaveTwoSubTopologies() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1;");

    // Then:
    assertThat(builder.build().describe().subtopologies(), hasSize(2));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithLegacyNameForRepartition() {
    // When:
    buildRequireRekey(
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(
            ImmutableMap.of(
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                String.valueOf(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF))));

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000010");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000007")));
    assertThat(
        node.topicSet(),
        hasItem(equalTo("KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition")));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithKsqlNameForRepartition() {
    // When:
    buildRequireRekey();

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000006")));
    assertThat(node.topicSet(), hasItem(equalTo("Aggregate-GROUP-BY-repartition")));
  }

  @Test
  public void shouldHaveSourceNodeForSecondSubtopolgyWithDefaultNameForRepartition() {
    buildRequireRekey(
        new KsqlConfig(
            ImmutableMap.of(
                StreamsConfig.TOPOLOGY_OPTIMIZATION,
                StreamsConfig.NO_OPTIMIZATION,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
        )
    );
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(),
        "KSTREAM-SOURCE-0000000010");
    final List<String> successors = node.successors().stream()
        .map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList("KSTREAM-AGGREGATE-0000000007")));
    assertThat(
        node.topicSet(),
        hasItem(containsString("KSTREAM-AGGREGATE-STATE-STORE-0000000006")));
    assertThat(node.topicSet(), hasItem(containsString("-repartition")));
  }

  @Test
  public void shouldHaveDefaultNameForAggregationStateStoreIfInternalTopicNamingOff() {
    build(
        new KsqlConfig(
            ImmutableMap.of(
                StreamsConfig.TOPOLOGY_OPTIMIZATION,
                StreamsConfig.NO_OPTIMIZATION,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
                KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_OFF)
        )
    );
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        builder.build(), "KSTREAM-AGGREGATE-0000000006");
    assertThat(node.stores(), hasItem(equalTo("KSTREAM-AGGREGATE-STATE-STORE-0000000005")));
  }

  @Test
  public void shouldHaveKsqlNameForAggregationStateStore() {
    build();
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        builder.build(), "KSTREAM-AGGREGATE-0000000005");
    assertThat(node.stores(), hasItem(equalTo("Aggregate-AGGREGATION")));
  }

  @Test
  public void shouldHaveSinkNodeWithSameTopicAsSecondSource() {
    // When:
    buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "GROUP BY col1;");

    // Then:
    final TopologyDescription.Sink sink = (TopologyDescription.Sink) getNodeByName(builder.build(), "KSTREAM-SINK-0000000007");
    final TopologyDescription.Source source = (TopologyDescription.Source) getNodeByName(builder.build(), "KSTREAM-SOURCE-0000000009");
    assertThat(sink.successors(), equalTo(Collections.emptySet()));
    assertThat(source.topicSet(), hasItem(sink.topic()));
  }

  @Test
  public void shouldBuildCorrectAggregateSchema() {
    // When:
    final SchemaKStream stream = buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 "
        + "window TUMBLING (size 2 second) "
        + "WHERE col0 > 100 GROUP BY col0;");

    // Then:
    assertThat(stream.getSchema().fields(), contains(
        new Field("COL0", 0, Schema.OPTIONAL_INT64_SCHEMA),
        new Field("KSQL_COL_1", 1, Schema.OPTIONAL_FLOAT64_SCHEMA),
        new Field("KSQL_COL_2", 2, Schema.OPTIONAL_INT64_SCHEMA)));
  }

  @Test
  public void shouldBeSchemaKTableResult() {
    final SchemaKStream stream = build();
    assertThat(stream.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldBeWindowedWhenStatementSpecifiesWindowing() {
    final SchemaKStream stream = build();
    assertThat(stream.getKeySerde(), is(not(Optional.empty())));
  }

  private SchemaKStream build() {
    return build(ksqlConfig);
  }

  private SchemaKStream build(final KsqlConfig ksqlConfig) {
    return buildQuery("SELECT col0, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "WHERE col0 > 100 GROUP BY col0;", ksqlConfig);
  }

  @SuppressWarnings("UnusedReturnValue")
  private SchemaKStream buildRequireRekey() {
    return buildRequireRekey(ksqlConfig);
  }

  @SuppressWarnings("UnusedReturnValue")
  private SchemaKStream buildRequireRekey(final KsqlConfig ksqlConfig) {
    return buildQuery("SELECT col1, sum(col3), count(col3) FROM test1 window TUMBLING ( "
        + "size 2 "
        + "second) "
        + "GROUP BY col1;", ksqlConfig);
  }

  private SchemaKStream buildQuery(final String queryString) {
    return buildQuery(queryString, ksqlConfig);
  }

  private SchemaKStream buildQuery(final String queryString, final KsqlConfig ksqlConfig) {
    return buildAggregateNode(queryString)
        .buildStream(
            builder,
            ksqlConfig,
            serviceContext,
            new InternalFunctionRegistry(),
            new HashMap<>());
  }

  private static AggregateNode buildAggregateNode(final String queryString) {
    final MetaStore newMetaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    final KsqlBareOutputNode planNode = (KsqlBareOutputNode) new LogicalPlanBuilder(newMetaStore)
        .buildLogicalPlan(queryString);

    return (AggregateNode) planNode.getSource();
  }
}
