/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class FilterNodeTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final FunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final PlanNodeId nodeId = new PlanNodeId("nodeid");
  private final Map<String, Object> props = Collections.emptyMap();
  private final QueryId queryId = new QueryId("queryid");

  @Mock
  private Expression predicate;
  @Mock
  private StreamsBuilder builder;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream schemaKStream;
  @Mock
  private ServiceContext serviceContext;

  private FilterNode node;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    when(sourceNode.buildStream(any(), any(), any(), any(), any(), any()))
        .thenReturn(schemaKStream);
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(schemaKStream.filter(any()))
        .thenReturn(schemaKStream);
    node = new FilterNode(nodeId, sourceNode, predicate);
  }

  @Test
  public void shouldApplyFilterCorrectly() {
    // When:
    node.buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        functionRegistry,
        props,
        queryId
    );

    // Then:
    verify(sourceNode).buildStream(
        same(builder),
        same(ksqlConfig),
        same(serviceContext),
        same(functionRegistry),
        same(props),
        same(queryId)
    );
    verify(schemaKStream).filter(predicate);
  }
}
