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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SuppressNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("nodeid");

  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKTable schemaKTable;
  @Mock
  private PlanBuildContext planBuildContext;
  @Mock
  private Stacker stacker;
  @Mock
  private KsqlWindowExpression ksqlWindowExpression;
  @Mock
  private DataSourceNode dataSourceNode;
  @Mock
  private DataSource dataSource;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private ValueFormat valueFormat;

  private SuppressNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setUp() {
    when(planBuildContext.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);
    when(sourceNode.getLeftmostSourceNode()).thenReturn(dataSourceNode);
    when(dataSourceNode.getDataSource()).thenReturn(dataSource);
    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(ksqlTopic.getValueFormat()).thenReturn(valueFormat);


  }

  @Test
  public void shouldThrowOnSuppressOnStream() {

    // Given:
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    node = new SuppressNode(NODE_ID, sourceNode, refinementInfo);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> node.buildStream(planBuildContext)
    );

    // Then
    assertThat(e.getMessage(), containsString("Failed in suppress node. Expected to find a Table, but found a stream instead"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSuppressOnSchemaKTable() {

    // Given:
    when(sourceNode.buildStream(any())).thenReturn(schemaKTable);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    node = new SuppressNode(NODE_ID, sourceNode, refinementInfo);

    // When:
    node.buildStream(planBuildContext);

    // Then
    verify(schemaKTable).suppress(refinementInfo, valueFormat.getFormatInfo(), stacker);
  }
}

