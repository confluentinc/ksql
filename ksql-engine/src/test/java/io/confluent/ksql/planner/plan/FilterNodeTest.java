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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.structured.SchemaKStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class FilterNodeTest {
  private final PlanNodeId nodeId = new PlanNodeId("nodeid");

  @Mock
  private Expression predicate;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream schemaKStream;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private FilterNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceNode.buildStream(any()))
        .thenReturn(schemaKStream);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(schemaKStream.filter(any(), any(), any()))
        .thenReturn(schemaKStream);

    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(ksqlStreamBuilder.buildNodeContext(nodeId)).thenReturn(stacker);


    node = new FilterNode(nodeId, sourceNode, predicate);
  }

  @Test
  public void shouldApplyFilterCorrectly() {
    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(sourceNode).buildStream(ksqlStreamBuilder);
    verify(schemaKStream).filter(predicate, stacker, processingLogContext);
  }
}
