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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
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
public class PreJoinProjectNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("1");

  private static final ColumnName K = ColumnName.of("K");
  private static final ColumnName COL_0 = ColumnName.of("col0");
  private static final ColumnName COL_1 = ColumnName.of("col1");
  private static final ColumnName COL_2 = ColumnName.of("col2");

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K, SqlTypes.STRING)
      .valueColumn(COL_0, SqlTypes.STRING)
      .valueColumn(COL_1, SqlTypes.STRING)
      .valueColumn(COL_2, SqlTypes.STRING)
      .build();

  private static final SourceName ALIAS = SourceName.of("a");

  @Mock
  private PreJoinRepartitionNode source;

  private PreJoinProjectNode projectNode;

  @Before
  public void setUp()  {
    when(source.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getSchema()).thenReturn(SCHEMA);

    projectNode = new PreJoinProjectNode(NODE_ID,
        source,
        ALIAS
    );
  }

  @Test
  public void shouldResolveStarSelectByCallingParent() {
    // Given:
    final Optional<SourceName> sourceName = Optional.of(SourceName.of("Bob"));

    // When:
    projectNode.resolveSelectStar(sourceName);

    // Then:
    verify(source).resolveSelectStar(sourceName);
  }

  @Test
  public void shouldAddAliasOnResolveSelectStarWhenAliased() {
    // Given:
    when(source.resolveSelectStar(any()))
        .thenReturn(ImmutableList.of(COL_1, COL_0, COL_2).stream());

    // When:
    final Stream<ColumnName> result = projectNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(
        ColumnNames.generatedJoinColumnAlias(ALIAS, COL_1),
        ColumnNames.generatedJoinColumnAlias(ALIAS, COL_0),
        ColumnNames.generatedJoinColumnAlias(ALIAS, COL_2)
    ));
  }

  @Test
  public void shouldNotAddAliasOnResolveSelectStarWhenNotAliased() {
    // Given:
    final ColumnName unknown = ColumnName.of("unknown");

    when(source.resolveSelectStar(any()))
        .thenReturn(ImmutableList.of(K, unknown).stream());

    // When:
    final Stream<ColumnName> result = projectNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(K, unknown));
  }
}