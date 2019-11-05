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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  private static final BooleanLiteral TRUE_EXPRESSION = new BooleanLiteral("true");
  private static final BooleanLiteral FALSE_EXPRESSION = new BooleanLiteral("false");
  private static final SelectExpression SELECT_0 = SelectExpression.of(ColumnName.of("col0"), TRUE_EXPRESSION);
  private static final SelectExpression SELECT_1 = SelectExpression.of(ColumnName.of("col1"), FALSE_EXPRESSION);
  private static final String KEY_FIELD_NAME = "col0";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("col0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("col1"), SqlTypes.STRING)
      .build();
  private static final KeyField SOURCE_KEY_FIELD = KeyField
      .of(ColumnRef.withoutSource(ColumnName.of("source-key")));

  @Mock
  private PlanNode source;
  @Mock
  private SchemaKStream<?> stream;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private ProjectNode projectNode;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(source.getKeyField()).thenReturn(SOURCE_KEY_FIELD);
    when(source.buildStream(any())).thenReturn((SchemaKStream) stream);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getSelectExpressions()).thenReturn(ImmutableList.of(SELECT_0, SELECT_1));
    when(ksqlStreamBuilder.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);
    when(stream.select(anyList(), any(), any())).thenReturn((SchemaKStream) stream);

    projectNode = new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of(ColumnRef.withoutSource(ColumnName.of(KEY_FIELD_NAME))));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfSchemaSizeDoesntMatchProjection() {
    when(source.getSelectExpressions()).thenReturn(ImmutableList.of(SELECT_0));
    new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of(ColumnRef.withoutSource(ColumnName.of(KEY_FIELD_NAME)))); // <-- not enough expressions
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnSchemaSelectNameMismatch() {
    when(source.getSelectExpressions()).thenReturn(ImmutableList.of(
        SelectExpression.of(ColumnName.of("wrongName"), TRUE_EXPRESSION),
        SELECT_1
    ));
    projectNode = new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of(ColumnRef.withoutSource(ColumnName.of(KEY_FIELD_NAME)))
    );
  }

  @Test
  public void shouldBuildSourceOnceWhenBeingBuilt() {
    // When:
    projectNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(source, times(1)).buildStream(ksqlStreamBuilder);
  }

  @Test
  public void shouldCreateProjectionWithFieldNameExpressionPairs() {
    // When:
    projectNode.buildStream(ksqlStreamBuilder);

    // Then:
    verify(stream).select(
        eq(ImmutableList.of(SELECT_0, SELECT_1)),
        eq(stacker),
        same(ksqlStreamBuilder)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfKeyFieldNameNotInSchema() {
    when(source.getSelectExpressions()).thenReturn(ImmutableList.of(SELECT_0, SELECT_1));
    new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of(ColumnRef.withoutSource(ColumnName.of("Unknown Key Field"))));
  }

  @Test
  public void shouldReturnKeyField() {
    // When:
    final KeyField keyField = projectNode.getKeyField();

    // Then:
    assertThat(keyField.ref(), is(Optional.of(ColumnRef.withoutSource(ColumnName.of(KEY_FIELD_NAME)))));
  }
}
