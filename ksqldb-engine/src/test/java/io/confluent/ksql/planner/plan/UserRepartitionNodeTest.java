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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.ValueFormat;
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
public class UserRepartitionNodeTest {

  private static final PlanNodeId PLAN_ID = new PlanNodeId("Blah");
  private static final SourceName SOURCE_NAME = SourceName.of("S1");
  private static final ColumnName K0 = ColumnName.of("K");
  private static final ColumnName V0 = ColumnName.of("V0");
  private static final ColumnName V1 = ColumnName.of("V1");
  private static final ColumnName V2 = ColumnName.of("V2");

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.DOUBLE)
      .valueColumn(V0, SqlTypes.DOUBLE)
      .valueColumn(V1, SqlTypes.DOUBLE)
      .valueColumn(V2, SqlTypes.DOUBLE)
      .valueColumn(K0, SqlTypes.DOUBLE)
      .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
      .build();

  @Mock
  private PlanNode parent;
  @Mock(name = "T.ID")
  private Expression originalPartitionBy;
  @Mock(name = "T_ID")
  private Expression rewrittenPartitionBy;
  @Mock
  private Projection projection;
  @Mock
  private DataSourceNode sourceNode;
  @Mock
  private DataSource source;
  @Mock
  private KsqlTopic topic;
  @Mock
  private ValueFormat valueFormat;

  private UserRepartitionNode repartitionNode;

  @Before
  public void setUp() {
    when(parent.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(parent.getSourceName()).thenReturn(Optional.of(SOURCE_NAME));
    when(parent.getSourceNodes()).thenReturn(Stream.of(sourceNode));
    when(sourceNode.getDataSource()).thenReturn(source);
    when(source.getKsqlTopic()).thenReturn(topic);
    when(topic.getValueFormat()).thenReturn(valueFormat);

    repartitionNode = new UserRepartitionNode(
        PLAN_ID,
        parent,
        SCHEMA,
        ImmutableList.of(originalPartitionBy),
        ImmutableList.of(rewrittenPartitionBy)
    );
  }

  @Test
  public void shouldResolveSelectStarIfSourceMatchesAndValuesOnly() {
    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.of(SOURCE_NAME)
    );

    // Then:
    assertThat(result.collect(Collectors.toList()), contains(K0, V0, V1, V2));
  }

  @Test
  public void shouldResolveSelectStarIfSourceNotProvidedAndValuesOnly() {
    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.empty()
    );

    // Then:
    final List<ColumnName> names = result.collect(Collectors.toList());
    assertThat(names, contains(K0, V0, V1, V2));
  }

  @Test
  public void shouldResolveSelectToPartitionByColumn() {
    // When:
    final Expression result = repartitionNode.resolveSelect(0, rewrittenPartitionBy);

    // Then:
    assertThat(result, is(new UnqualifiedColumnReferenceExp(K0)));
  }

  @Test
  public void shouldThrowIfProjectionMissingPartitionBy() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> repartitionNode.validateKeyPresent(SOURCE_NAME, projection)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The query used to build `S1` "
        + "must include the partitioning expression T.ID in its projection (eg, SELECT T.ID..."));
  }

  @Test
  public void shouldNotThrowIfProjectionHasPartitionBy() {
    // Given:
    when(projection.containsExpression(rewrittenPartitionBy)).thenReturn(true);

    // When:
    repartitionNode.validateKeyPresent(SOURCE_NAME, projection);

    // Then: did not throw.
  }
}