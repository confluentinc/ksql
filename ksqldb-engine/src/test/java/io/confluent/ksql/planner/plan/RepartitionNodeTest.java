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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
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
public class RepartitionNodeTest {

  private static final PlanNodeId PLAN_ID = new PlanNodeId("Blah") ;
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

  private static final List<ColumnName> PARENT_COL_NAMES = ImmutableList.of(
    ColumnName.of("this"),
    ColumnName.of("that")
  );

  @Mock
  private PlanNode parent;
  @Mock(name = "T.ID")
  private Expression originalPartitionBy;
  @Mock(name = "T_ID")
  private Expression rewrittenPartitionBy;
  @Mock
  private Projection projection;

  private RepartitionNode repartitionNode;

  @Before
  public void setUp() {
    when(parent.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(parent.getSourceName()).thenReturn(Optional.of(SOURCE_NAME));
    when(parent.resolveSelectStar(any())).thenReturn(PARENT_COL_NAMES.stream());

    repartitionNode = new RepartitionNode(PLAN_ID, parent, SCHEMA, originalPartitionBy,
        rewrittenPartitionBy, false);
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
  public void shouldPassResolveSelectStarToParentIfInternal() {
    // Given:
    givenInternalRepartition();

    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.empty()
    );

    // Then:
    verify(parent).resolveSelectStar(Optional.empty());

    assertThat(result.collect(Collectors.toList()), is(PARENT_COL_NAMES));
  }

  @Test
  public void shouldResolveSelectToPartitionByColumn() {
    // Given:
    givenAnyKeyEnabled();

    // When:
    final Expression result = repartitionNode.resolveSelect(0, rewrittenPartitionBy);

    // Then:
    assertThat(result, is(new UnqualifiedColumnReferenceExp(K0)));
  }

  @Test
  public void shouldThrowIfProjectionMissingPartitionBy() {
    // Given:
    givenAnyKeyEnabled();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> repartitionNode.validateKeyPresent(SOURCE_NAME, projection)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The query used to build `S1` "
        + "must include the partitioning expression T.ID in its projection."));
  }

  @Test
  public void shouldNotThrowIfProjectionHasPartitionBy() {
    // Given:
    givenAnyKeyEnabled();
    when(projection.containsExpression(rewrittenPartitionBy)).thenReturn(true);

    // When:
    repartitionNode.validateKeyPresent(SOURCE_NAME, projection);

    // Then: did not throw.
  }

  private void givenAnyKeyEnabled() {
    repartitionNode = new RepartitionNode(PLAN_ID, parent, SCHEMA, originalPartitionBy,
        rewrittenPartitionBy, false);
  }

  private void givenInternalRepartition() {
    repartitionNode = new RepartitionNode(PLAN_ID, parent, SCHEMA, originalPartitionBy,
        rewrittenPartitionBy, true);
  }
}