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
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
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
  private static final SourceName MATCHING_SOURCE_NAME = SourceName.of("S1");
  private static final ColumnName K0 = ColumnName.of("K");
  private static final ColumnName V0 = ColumnName.of("V0");
  private static final ColumnName V1 = ColumnName.of("V1");
  private static final ColumnName V2 = ColumnName.of("V2");
  private static final Optional<ColumnName> PARTITION_BY_ALIAS =
      Optional.of(ColumnName.of("SomeAlias"));

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.DOUBLE)
      .valueColumn(V0, SqlTypes.DOUBLE)
      .valueColumn(V1, SqlTypes.DOUBLE)
      .valueColumn(V2, SqlTypes.DOUBLE)
      .valueColumn(K0, SqlTypes.DOUBLE)
      .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
      .build();

  private static final List<ColumnName> PARENT_COL_NAMES = ImmutableList.of(
    ColumnName.of("this"),
    ColumnName.of("that")
  );

  @Mock
  private PlanNode parent;
  @Mock
  private PartitionBy partitionBy;
  @Mock
  private Expression partitionByExpression;
  private RepartitionNode repartitionNode;

  @Before
  public void setUp() {
    when(parent.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(parent.getSourceName()).thenReturn(Optional.of(MATCHING_SOURCE_NAME));
    when(parent.resolveSelectStar(any(), anyBoolean())).thenReturn(PARENT_COL_NAMES.stream());

    when(partitionBy.getAlias()).thenReturn(PARTITION_BY_ALIAS);
    when(partitionBy.getExpression()).thenReturn(partitionByExpression);

    repartitionNode = new RepartitionNode(PLAN_ID, parent, SCHEMA, partitionBy, KeyField.none());
  }

  @Test
  public void shouldResolveSelectStarIfSourceMatchesAndValuesOnly() {
    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.of(MATCHING_SOURCE_NAME),
        true
    );

    // Then:
    final List<ColumnName> names = result.collect(Collectors.toList());
    assertThat(names, contains(V0, V1, V2));
  }

  @Test
  public void shouldResolveSelectStarIfSourceNotProvidedAndValuesOnly() {
    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.empty(),
        true
    );

    // Then:
    final List<ColumnName> names = result.collect(Collectors.toList());
    assertThat(names, contains(V0, V1, V2));
  }

  @Test
  public void shouldPassResolveSelectStarToParentIfNotValuesOnly() {
    // When:
    final Stream<ColumnName> result = repartitionNode.resolveSelectStar(
        Optional.empty(),
        false
    );

    // Then:
    final List<ColumnName> names = result.collect(Collectors.toList());
    assertThat(names, is(PARENT_COL_NAMES));

    verify(parent).resolveSelectStar(Optional.empty(), false);
  }
}