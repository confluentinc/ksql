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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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

  private static final LogicalSchema SOURCE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.STRING)
      .valueColumn(COL_2, SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K"), SqlTypes.STRING)
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
  private KsqlQueryBuilder ksqlStreamBuilder;
  @Mock
  private Stacker stacker;

  private ProjectNode projectNode;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Before
  public void init() {
    when(source.buildStream(any())).thenReturn((SchemaKStream) stream);
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlStreamBuilder.buildNodeContext(NODE_ID.toString())).thenReturn(stacker);
    when(stream.select(any(), any(), any())).thenReturn((SchemaKStream) stream);

    projectNode = new ProjectNode(
        NODE_ID,
        source,
        SELECTS,
        SCHEMA,
        false
    );
  }

  @Test
  public void shouldThrowIfSchemaHasNoValueColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("bob"), SqlTypes.STRING)
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> new ProjectNode(
            NODE_ID,
            source,
            ImmutableList.of(),
            schema,
            false
        )
    );

    // Then:
    assertThat(e.getMessage(), is("The projection contains no value columns."));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaSizeDoesntMatchProjection() {
    new ProjectNode(
        NODE_ID,
        source,
        ImmutableList.of(SELECT_0), // <-- not enough expressions
        SCHEMA,
        false
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnSchemaSelectNameMismatch() {
    projectNode = new ProjectNode(
        NODE_ID,
        source,
        ImmutableList.of(
            SelectExpression.of(ColumnName.of("wrongName"), new BooleanLiteral("true")),
            SELECT_1,
            SELECT_2
        ),
        SCHEMA,
        false
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
        eq(SELECTS),
        eq(stacker),
        same(ksqlStreamBuilder)
    );
  }

  @Test
  public void shouldResolveStarSelectByCallingParent() {
    // Given:
    final Optional<SourceName> source = Optional.of(SourceName.of("Bob"));

    // When:
    projectNode.resolveSelectStar(source);

    // Then:
    verify(this.source).resolveSelectStar(source);
  }

  @Test
  public void shouldResolveStarSelectWhenUnaliased() {
    // Given:
    when(source.resolveSelectStar(any()))
        .thenReturn(ImmutableList.of(COL_1, COL_0, COL_2).stream());

    // When:
    final Stream<ColumnName> result = projectNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL_1, COL_0, COL_2));
  }

  @Test
  public void shouldResolveStarSelectWhenAliased() {
    // Given:
    projectNode = new ProjectNode(
        NODE_ID,
        source,
        SELECTS,
        SCHEMA,
        true
    );

    when(source.resolveSelectStar(any()))
        .thenReturn(ImmutableList.of(COL_1, COL_0, COL_2).stream());

    // When:
    final Stream<ColumnName> result = projectNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL_1, COL_0, ALIASED_COL_2));
  }

  @Test
  public void shouldThrowOnMultipleKeyColumns() {
    // Given:
    final LogicalSchema badSchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("K"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("SecondKey"), SqlTypes.STRING)
        .valueColumn(COL_0, SqlTypes.STRING)
        .valueColumn(COL_1, SqlTypes.STRING)
        .valueColumn(ALIASED_COL_2, SqlTypes.STRING)
        .build();

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> {
          new ProjectNode(
              NODE_ID,
              source,
              SELECTS,
              badSchema,
              true
          );
        }
    );

    // Then:
    assertThat(e.getMessage(), containsString("The projection contains the key column "
        + "more than once: `K` and `SecondKey`."));
  }
}
