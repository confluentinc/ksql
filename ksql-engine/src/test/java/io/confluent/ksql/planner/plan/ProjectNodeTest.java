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
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.QueryContext.Stacker;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SelectExpression;
import java.util.Arrays;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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
  private static final String KEY_FIELD_NAME = "field1";
  private static final LogicalSchema SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_STRING_SCHEMA)
      .build());
  private static final KeyField SOURCE_KEY_FIELD = KeyField
      .of("source-key", Field.of("legacy-source-key", SqlTypes.STRING));

  @Mock
  private PlanNode source;
  @Mock
  private SchemaKStream<?> stream;
  @Mock
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;
  @Mock
  private ProcessingLogContext processingLogContext;

  private ProjectNode projectNode;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    when(source.getKeyField()).thenReturn(SOURCE_KEY_FIELD);
    when(source.buildStream(any())).thenReturn((SchemaKStream) stream);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlStreamBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(ksqlStreamBuilder.buildNodeContext(NODE_ID)).thenReturn(stacker);
    when(stream.select(anyList(), any(), any())).thenReturn((SchemaKStream) stream);

    projectNode = new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of(KEY_FIELD_NAME),
        ImmutableList.of(TRUE_EXPRESSION, FALSE_EXPRESSION));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfSchemaSizeDoesntMatchProjection() {
    new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of("field1"),
        ImmutableList.of(TRUE_EXPRESSION)); // <-- not enough expressions
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
        eq(Arrays.asList(
            SelectExpression.of("field1", TRUE_EXPRESSION),
            SelectExpression.of("field2", FALSE_EXPRESSION))),
        eq(stacker),
        same(processingLogContext)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfKeyFieldNameNotInSchema() {
    new ProjectNode(
        NODE_ID,
        source,
        SCHEMA,
        Optional.of("Unknown Key Field"),
        ImmutableList.of(TRUE_EXPRESSION, FALSE_EXPRESSION));
  }

  @Test
  public void shouldReturnKeyField() {
    // When:
    final KeyField keyField = projectNode.getKeyField();

    // Then:
    assertThat(keyField.name(), is(Optional.of(KEY_FIELD_NAME)));
    assertThat(keyField.legacy(), is(SOURCE_KEY_FIELD.legacy()));
  }
}
