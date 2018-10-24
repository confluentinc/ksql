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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class SelectValueMapperTest {

  private final MetaStore metaStore =
      MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(metaStore);

  @Test
  public void shouldSelectChosenColumns() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow transformed = selectMapper.apply(
        genericRow(1521834663L, "key1", 1L, "hi", "bye", 2.0F, "blah"));

    // Then:
    assertThat(transformed, is(genericRow(1L, "bye", 2.0F)));
  }

  @Test
  public void shouldApplyUdfsToColumns() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow row = selectMapper.apply(
        genericRow(1521834663L, "key1", 2L, "foo", "whatever", 6.9F, "boo", "hoo"));

    // Then:
    assertThat(row, is(genericRow(2L, "foo", "whatever", 7.0F)));
  }

  @Test
  public void shouldHandleNullRows() {
    // Given:
    final SelectValueMapper selectMapper = givenSelectMapperFor(
        "SELECT col0, col1, col2, CEIL(col3) FROM test1 WHERE col0 > 100;");

    // When:
    final GenericRow row = selectMapper.apply(null);

    // Then:
    assertThat(row, is(nullValue()));
  }

  private SelectValueMapper givenSelectMapperFor(final String query) {
    final PlanNode planNode = planBuilder.buildLogicalPlan(query);
    final ProjectNode projectNode = (ProjectNode) planNode.getSources().get(0);
    final Schema schema = planNode.getTheSourceNode().getSchema();
    final List<SelectExpression> selectExpressions = projectNode.getProjectSelectExpressions();
    final List<ExpressionMetadata> metadata = createExpressionMetadata(selectExpressions, schema);
    final List<String> selectFieldNames = selectExpressions.stream()
        .map(SelectExpression::getName)
        .collect(Collectors.toList());
    final GenericRowValueTypeEnforcer typeEnforcer = new GenericRowValueTypeEnforcer(schema);
    return new SelectValueMapper(typeEnforcer, selectFieldNames, metadata);
  }

  private List<ExpressionMetadata> createExpressionMetadata(
      final List<SelectExpression> selectExpressions,
      final Schema schema
  ) {
    try {
      final CodeGenRunner codeGenRunner = new CodeGenRunner(
          schema, new InternalFunctionRegistry());

      final List<ExpressionMetadata> expressionEvaluators = new ArrayList<>();
      for (final SelectExpression expressionPair : selectExpressions) {
        expressionEvaluators
            .add(codeGenRunner.buildCodeGenFromParseTree(expressionPair.getExpression()));
      }
      return expressionEvaluators;
    } catch (final Exception e) {
      throw new AssertionError("Invalid test", e);
    }
  }

  private static GenericRow genericRow(final Object... columns) {
    return new GenericRow(Arrays.asList(columns));
  }
}
