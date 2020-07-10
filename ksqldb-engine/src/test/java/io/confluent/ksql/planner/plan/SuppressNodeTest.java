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

import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.parser.ResultMaterialization;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
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
  private ResultMaterialization resultMaterialization;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream schemaKStream;
  @Mock
  private SchemaKTable schemaKTable;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private SuppressNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowOnSuppressOnStream() {

    // When:
    when(sourceNode.getSchema()).thenReturn(LogicalSchema.builder().build());
    when(sourceNode.buildStream(any()))
        .thenReturn(schemaKStream);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(schemaKTable.suppress(any(), any()))
        .thenReturn(schemaKTable);

    when(ksqlStreamBuilder.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);

    node = new SuppressNode(NODE_ID, sourceNode, resultMaterialization);

    final Exception e = assertThrows(
        KsqlException.class,
        () -> node.buildStream(ksqlStreamBuilder)
    );

    // Then
    assertThat(e.getMessage(), containsString("Failed to build suppress node. Expected to find a Table"));
  }
}

