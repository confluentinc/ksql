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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class FilterNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("nodeid");

  @Mock
  private Expression predicate;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream schemaKStream;
  @Mock
  private PlanBuildContext planBuildContext;
  @Mock
  private Stacker stacker;

  private FilterNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceNode.getSchema()).thenReturn(LogicalSchema.builder().build());
    when(sourceNode.buildStream(any()))
        .thenReturn(schemaKStream);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(schemaKStream.filter(any(), any()))
        .thenReturn(schemaKStream);

    when(planBuildContext.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);

    node = new FilterNode(NODE_ID, sourceNode, predicate);
  }

  @Test
  public void shouldApplyFilterCorrectly() {
    // When:
    node.buildStream(planBuildContext);

    // Then:
    verify(sourceNode).buildStream(planBuildContext);
    verify(schemaKStream).filter(predicate, stacker);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  @Test
  public void shouldUseRightOpName() {
    // When:
    node.buildStream(planBuildContext);

    // Then:
    verifyNoMoreInteractions(stacker);
  }
}
