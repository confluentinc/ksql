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

package io.confluent.ksql.structured;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SelectValueMapperTest {

  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);


  @Test
  public void shouldSelectChosenColumns() throws Exception {
    final SelectValueMapper mapper = createMapper("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");
    final GenericRow transformed = mapper.apply(new GenericRow(Arrays.asList(1521834663L,
                                                                             "key1", 1L, "hi", "bye", 2.0F, "blah")));
    assertThat(transformed, equalTo(new GenericRow(Arrays.asList(1L, "bye", 2.0F))));
  }

  @Test
  public void shouldApplyUdfsToColumns() throws Exception {
    final SelectValueMapper mapper = createMapper("SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");
    final GenericRow row = mapper.apply(new GenericRow(Arrays.asList(1521834663L, "key1", 2L,
                                                                     "foo",
        "whatever", 6.9F, "boo", "hoo")));
    assertThat(row, equalTo(new GenericRow(Arrays.asList(2L, "foo", "whatever",
                                                         7.0F))));
  }

  private SelectValueMapper createMapper(final String query) throws Exception {
    final PlanNode planNode = planBuilder.buildLogicalPlan(query);
    final ProjectNode projectNode = (ProjectNode) planNode.getSources().get(0);
    final Schema schema = planNode.getTheSourceNode().getSchema();
    final List<Pair<String, Expression>> expressionPairList = projectNode.getProjectNameExpressionPairList();
    final List<ExpressionMetadata> metadata = createExpressionMetadata(expressionPairList, schema);
    return new SelectValueMapper(new GenericRowValueTypeEnforcer(schema), expressionPairList, metadata);
  }


  private List<ExpressionMetadata> createExpressionMetadata(final List<Pair<String, Expression>> expressionPairList,
                                                            final Schema schema) throws Exception {
    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, new FunctionRegistry());
    final List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
    for (Pair<String, Expression> expressionPair : expressionPairList) {
      final ExpressionMetadata
          expressionEvaluator =
          codeGenRunner.buildCodeGenFromParseTree(expressionPair.getRight());
      expressionEvaluators.add(expressionEvaluator);
    }
    return expressionEvaluators;
  }
}