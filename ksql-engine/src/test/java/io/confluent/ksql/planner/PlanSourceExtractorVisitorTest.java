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

package io.confluent.ksql.planner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;

import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class PlanSourceExtractorVisitorTest {

  private MetaStore metaStore;
  private KsqlConfig ksqlConfig;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    ksqlConfig = new KsqlConfig(Collections.emptyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForSimpleQuery() {
    final PlanNode planNode = buildLogicalPlan("select col0 from TEST2 limit 5;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames.size(), equalTo(1));
    assertThat(sourceNames, equalTo(Utils.mkSet("TEST2")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForJoinQuery() {
    final PlanNode planNode = buildLogicalPlan(
        "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN "
                          + "test2 t2 ON t1.col1 = t2.col1;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames, equalTo(Utils.mkSet("TEST1", "TEST2")));
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
  }
}
