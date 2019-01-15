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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SelectExpression;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ProjectNodeTest {

  @Mock
  private PlanNode source;
  @Mock
  private SchemaKStream stream;

  private final StreamsBuilder builder = new StreamsBuilder();
  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final ServiceContext serviceContext = TestServiceContext.create();
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final HashMap<String, Object> props = new HashMap<>();
  private final QueryId queryId = new QueryId("project-test");

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void init() {
    mockSourceNode();
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
  }

  @After
  public void tearDown() {
    serviceContext.close();
  }

  private ProjectNode buildNode(final List<Expression> expressionList) {
    return new ProjectNode(
        new PlanNodeId("1"),
        source,
        SchemaBuilder.struct()
            .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
            .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
            .build(),
        expressionList);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowKsqlExcptionIfSchemaSizeDoesntMatchProjection() {
    final ProjectNode node = buildNode(
        Collections.singletonList(new BooleanLiteral("true")));

    node.buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        functionRegistry,
        props,
        queryId);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCreateProjectionWithFieldNameExpressionPairs() {
    // Given:
    final BooleanLiteral trueExpression = new BooleanLiteral("true");
    final BooleanLiteral falseExpression = new BooleanLiteral("false");
    when(stream.select(anyList())).thenReturn(stream);
    final ProjectNode node = buildNode(
        Arrays.asList(trueExpression, falseExpression));

    // When:
    node.buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        functionRegistry,
        props,
        queryId);

    // Then:
    verify(stream).select(
        Arrays.asList(
            SelectExpression.of("field1", trueExpression),
            SelectExpression.of("field2", falseExpression))
    );
    verify(source, times(1)).buildStream(
        same(builder),
        same(ksqlConfig),
        same(serviceContext),
        same(functionRegistry),
        same(props),
        same(queryId)
    );
  }

  @SuppressWarnings("unchecked")
  private void mockSourceNode() {
    when(source.getKeyField())
        .thenReturn(new Field("field1", 0, Schema.OPTIONAL_STRING_SCHEMA));
    when(source.buildStream(
        any(StreamsBuilder.class),
        any(KsqlConfig.class),
        any(ServiceContext.class),
        any(InternalFunctionRegistry.class),
        anyMap(),
        same(queryId))
    ).thenReturn(stream);
  }
}
