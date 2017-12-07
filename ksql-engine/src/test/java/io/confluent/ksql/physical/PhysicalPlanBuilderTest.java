/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.physical;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.QueryMetadata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class PhysicalPlanBuilderTest {
  private final String simpleSelectFilter = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
  private PhysicalPlanBuilder physicalPlanBuilder;
  private MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private LogicalPlanBuilder planBuilder;

  @Before
  public void before() {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final FunctionRegistry functionRegistry = new FunctionRegistry();
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    physicalPlanBuilder = new PhysicalPlanBuilder(streamsBuilder,
        new KsqlConfig(configMap),
        new FakeKafkaTopicClient(),
        new MetastoreUtil(),
        functionRegistry,
        Collections.emptyMap(),
        false,
        metaStore
    );

    planBuilder = new LogicalPlanBuilder(metaStore);
  }

  private QueryMetadata buildPhysicalPlan(final String query) throws Exception {
    final PlanNode logical = planBuilder.buildLogicalPlan(query);
    return physicalPlanBuilder.buildPhysicalPlan(new Pair<>(query, logical));
  }

  @Test
  public void shouldHaveKStreamDataSource() throws Exception {
    final QueryMetadata metadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(metadata.getDataSourceType(), equalTo(DataSource.DataSourceType.KSTREAM));
  }

  @Test
  public void shouldHaveOutputNode() throws Exception {
    final QueryMetadata queryMetadata = buildPhysicalPlan(simpleSelectFilter);
    assertThat(queryMetadata.getOutputNode(), instanceOf(KsqlBareOutputNode.class));
  }

  @Test
  public void shouldCreateExecutionPlan() throws Exception {
    String queryString = "SELECT col0, sum(col3), count(col3) FROM test1 "
        + "WHERE col0 > 100 GROUP BY col0;";
    final QueryMetadata metadata = buildPhysicalPlan(queryString);
    final String planText = metadata.getExecutionPlan();
    String[] lines = planText.split("\n");
    Assert.assertEquals(lines[0], " > [ SINK ] Schema: [COL0 : INT64 , KSQL_COL_1 : FLOAT64 "
        + ", KSQL_COL_2 : INT64].");
    Assert.assertEquals(lines[1], "\t\t > [ AGGREGATE ] Schema: [TEST1.COL0 : INT64 , TEST1.COL3 : FLOAT64 , KSQL_AGG_VARIABLE_0 : FLOAT64 , KSQL_AGG_VARIABLE_1 : INT64].");
    Assert.assertEquals(lines[2], "\t\t\t\t > [ PROJECT ] Schema: [TEST1.COL0 : INT64 , TEST1.COL3 : FLOAT64].");
    Assert.assertEquals(lines[3], "\t\t\t\t\t\t > [ REKEY ] Schema: [TEST1.COL0 : INT64 , TEST1.COL1 : STRING , TEST1.COL2 : STRING , TEST1.COL3 : FLOAT64 , TEST1.COL4 : ARRAY , TEST1.COL5 : MAP].");
    Assert.assertEquals(lines[4], "\t\t\t\t\t\t\t\t > [ FILTER ] Schema: [TEST1.COL0 : INT64 , TEST1.COL1 : STRING , TEST1.COL2 : STRING , TEST1.COL3 : FLOAT64 , TEST1.COL4 : ARRAY , TEST1.COL5 : MAP].");
    Assert.assertEquals(lines[5], "\t\t\t\t\t\t\t\t\t\t > [ SOURCE ] Schema: [TEST1.COL0 : INT64 , TEST1.COL1 : STRING , TEST1.COL2 : STRING , TEST1.COL3 : FLOAT64 , TEST1.COL4 : ARRAY , TEST1.COL5 : MAP].");
  }

}
