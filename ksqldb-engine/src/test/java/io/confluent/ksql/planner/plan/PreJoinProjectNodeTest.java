/*
 * Copyright 2020 Confluent Inc.
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
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class PreJoinProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");

  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName COL_0 = ColumnName.of("col0");
  private static final ColumnName COL_1 = ColumnName.of("col1");
  private static final ColumnName COL_2 = ColumnName.of("col2");
  private static final ColumnName ALIASED_COL_2 = ColumnName.of("a_col2");

  private static final SelectExpression SELECT_0 = SelectExpression
      .of(COL_0, new BooleanLiteral("true"));
  private static final SelectExpression SELECT_1 = SelectExpression
      .of(COL_1, new BooleanLiteral("false"));
  private static final SelectExpression SELECT_2 = SelectExpression
      .of(ALIASED_COL_2, new UnqualifiedColumnReferenceExp(COL_2));

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.STRING)
      .valueColumn(COL_0, SqlTypes.STRING)
      .valueColumn(COL_1, SqlTypes.STRING)
      .valueColumn(ALIASED_COL_2, SqlTypes.STRING)
      .build();

  private static final List<SelectExpression> SELECTS = ImmutableList.of(
      SELECT_0,
      SELECT_1,
      SELECT_2
  );

  @Mock
  private PlanNode source;
  @Mock
  private SchemaKStream<?> stream;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private PreJoinProjectNode projectNode;

  @Before
  public void setUp()  {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);

    projectNode = new PreJoinProjectNode(NODE_ID,
        source,
        SELECTS,
        SCHEMA);
  }

  @Test
  public void shouldResolveStarSelectByCallingParent() {
    // Given:
    final Optional<SourceName> sourceName = Optional.of(SourceName.of("Bob"));

    // When:
    projectNode.resolveSelectStar(sourceName);

    // Then:
    verify(source).resolveSelectStar(sourceName);
  }

  @Test
  public void shouldResolveStarSelectWhenAliased() {
    // Given:
    when(source.resolveSelectStar(any()))
        .thenReturn(ImmutableList.of(COL_1, COL_0, COL_2).stream());

    // When:
    final Stream<ColumnName> result = projectNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL_1, COL_0, ALIASED_COL_2));
  }
}