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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SelectExpression;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");
  @Mock
  private PlanNode source;
  @Mock
  private SchemaKStream<?> stream;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private final ServiceContext serviceContext = TestServiceContext.create();
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(source.buildStream(any())).thenReturn((SchemaKStream) stream);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(ksqlStreamBuilder.buildNodeContext(NODE_ID)).thenReturn(stacker);
  }

  @After
  public void tearDown() {
    serviceContext.close();
  }

  private ProjectNode buildNode(final List<Expression> expressionList) {
    return new ProjectNode(
        NODE_ID,
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

    node.buildStream(ksqlStreamBuilder);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateProjectionWithFieldNameExpressionPairs() {
    // Given:
    final BooleanLiteral trueExpression = new BooleanLiteral("true");
    final BooleanLiteral falseExpression = new BooleanLiteral("false");
    when(stream.select(anyList(), any(), any())).thenReturn((SchemaKStream) stream);
    final ProjectNode node = buildNode(
        Arrays.asList(trueExpression, falseExpression));

    // When:
    node.buildStream(ksqlStreamBuilder);

    // Then:
    verify(stream).select(
        eq(Arrays.asList(
            SelectExpression.of("field1", trueExpression),
            SelectExpression.of("field2", falseExpression))),
        eq(stacker),
        same(processingLogContext)
    );
    verify(source, times(1)).buildStream(ksqlStreamBuilder);
  }
}
