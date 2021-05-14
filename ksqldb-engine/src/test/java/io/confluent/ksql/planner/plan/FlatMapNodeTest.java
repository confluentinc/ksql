/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdtfLoader;
import io.confluent.ksql.function.udtf.array.Explode;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FlatMapNodeTest {

  private static final PlanNodeId PLAN_ID = new PlanNodeId("Bob");

  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName ARRAY_COL = ColumnName.of("ARRAY_COL");

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ARRAY_COL, SqlTypes.array(SqlTypes.INTEGER))
      .valueColumn(COL0, SqlTypes.STRING)
      .build();

  private static final FunctionCall A_TABLE_FUNCTION = new FunctionCall(
      FunctionName.of("EXPLODE"),
      ImmutableList.of(new UnqualifiedColumnReferenceExp(ARRAY_COL))
  );

  @Mock
  private PlanNode source;
  @Mock
  private ImmutableAnalysis analysis;

  private FlatMapNode flatMapNode;

  @Before
  public void setUp() {
    when(source.getSchema()).thenReturn(SOURCE_SCHEMA);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);

    when(analysis.getTableFunctions())
        .thenReturn(ImmutableList.of(A_TABLE_FUNCTION));

    when(analysis.getSelectItems())
        .thenReturn(ImmutableList.of(
            new AllColumns(Optional.empty()),
            new SingleColumn(new UnqualifiedColumnReferenceExp(COL0), Optional.empty()),
            new SingleColumn(A_TABLE_FUNCTION, Optional.empty())
        ));

    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();

    new UdtfLoader(functionRegistry, Optional.empty(), SqlTypeParser.create(TypeRegistry.EMPTY),
        true)
        .loadUdtfFromClass(Explode.class, "load path");

    flatMapNode = new FlatMapNode(
        PLAN_ID,
        source,
        functionRegistry,
        analysis
    );
  }

  @Test
  public void shouldResolveNoneUdtfSelectExpressionToSelf() {
    // Given:
    final Expression exp = mock(Expression.class);

    // When:
    final Expression result = flatMapNode.resolveSelect(0, exp);

    // Then:
    assertThat(result, is(exp));
  }

  @Test
  public void shouldResolveUdtfSelectExpressionToInternalName() {
    // Given:
    final Expression exp = mock(Expression.class);

    // When:
    final Expression result = flatMapNode.resolveSelect(2, exp);

    // Then:
    assertThat(result, is(new UnqualifiedColumnReferenceExp(ColumnName.of("KSQL_SYNTH_0"))));
  }
}