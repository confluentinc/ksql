/**
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

package io.confluent.ksql.planner;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.structured.LogicalPlanBuilder;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class PlanSourceExtractorVisitorTest {

  private final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private FunctionRegistry functionRegistry;
  private LogicalPlanBuilder logicalPlanBuilder;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
    logicalPlanBuilder = new LogicalPlanBuilder(metaStore);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForSimpleQuery() {
    final PlanNode planNode = logicalPlanBuilder.buildLogicalPlan("select col0 from TEST2 limit 5;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames.size(), equalTo(1));
    assertThat(sourceNames, equalTo(Utils.mkSet("TEST2")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForJoinQuery() {
    final PlanNode planNode = logicalPlanBuilder.buildLogicalPlan(
        "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN "
                          + "test2 t2 ON t1.col1 = t2.col1;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<String> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames, equalTo(Utils.mkSet("TEST1", "TEST2")));
  }

}
