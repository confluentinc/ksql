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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProjectNodeTest {

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
  private PlanBuildContext planBuildContext;
  @Mock
  private Stacker stacker;
  @Mock
  private FormatInfo internalFormats;

  private ProjectNode projectNode;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void init() {
    when(source.buildStream(any())).thenReturn((SchemaKStream) stream);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(planBuildContext.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);
    when(stream.select(any(), any(), any(), any(), any())).thenReturn((SchemaKStream) stream);

    projectNode = spy(new TestProjectNode(
        NODE_ID,
        source,
        SELECTS,
        SCHEMA
    ));

    doReturn(internalFormats).when(projectNode).getFormatInfo();
  }

  @Test
  public void shouldBuildSourceOnceWhenBeingBuilt() {
    // When:
    projectNode.buildStream(planBuildContext);

    // Then:
    verify(source, times(1)).buildStream(planBuildContext);
  }

  @Test
  public void shouldSelectProjection() {
    // When:
    projectNode.buildStream(planBuildContext);

    // Then:
    verify(stream).select(
        ImmutableList.of(K),
        SELECTS,
        stacker,
        planBuildContext,
        internalFormats
    );
  }

  private static final class TestProjectNode extends ProjectNode {

    private final List<SelectExpression> projection;
    private final LogicalSchema schema;

    public TestProjectNode(
        final PlanNodeId id,
        final PlanNode source,
        final List<SelectExpression> projection,
        final LogicalSchema schema
    ) {
      super(id, source);
      this.projection = projection;
      this.schema = schema;
    }

    @Override
    public List<SelectExpression> getSelectExpressions() {
      return projection;
    }

    @Override
    public LogicalSchema getSchema() {
      return schema;
    }
  }
}
