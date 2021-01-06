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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
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
public class PlanNodeTest {

  private static final PlanNodeId NODE_ID = new PlanNodeId("Bob");
  private static final LogicalSchema SCHEMA = LogicalSchema.builder().build();
  private static final SourceName SOURCE_1_NAME = SourceName.of("S1");
  private static final SourceName SOURCE_2_NAME = SourceName.of("S2");
  private static final ColumnName COL0 = ColumnName.of("C0");
  private static final ColumnName COL1 = ColumnName.of("C1");
  private static final ColumnName COL2 = ColumnName.of("C2");
  private static final ColumnName COL3 = ColumnName.of("C3");

  @Mock
  private PlanNode source1;

  @Mock
  private PlanNode source2;

  private TestPlanNode planNode;

  @Before
  public void setUp() {
    planNode = new TestPlanNode(NODE_ID, DataSourceType.KSTREAM, Optional.empty());

    when(source1.getSourceName()).thenReturn(Optional.of(SOURCE_1_NAME));
    when(source2.getSourceName()).thenReturn(Optional.of(SOURCE_2_NAME));

    when(source1.resolveSelectStar(any())).thenReturn(Stream.of(COL0, COL1));
    when(source2.resolveSelectStar(any())).thenReturn(Stream.of(COL2, COL3));
  }

  @Test
  public void shouldResolvingSelectAsNoOp() {
    // Given:
    final Expression exp = mock(Expression.class);

    // Then:
    assertThat(planNode.resolveSelect(10, exp), is(exp));
  }

  @Test
  public void shouldResolveUnaliasedSelectStarByCallingAllSources() {
    // When:
    final Stream<ColumnName> result = planNode.resolveSelectStar(Optional.empty());

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL0, COL1, COL2, COL3));

    verify(source1).resolveSelectStar(Optional.empty());
    verify(source2).resolveSelectStar(Optional.empty());
  }

  @Test
  public void shouldResolveAliasedSelectStarByCallingOnlyCorrectParent() {
    // When:
    final Stream<ColumnName> result = planNode.resolveSelectStar(Optional.of(SOURCE_2_NAME));

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL2, COL3));

    verify(source1, never()).resolveSelectStar(any());
    verify(source2).resolveSelectStar(Optional.of(SOURCE_2_NAME));
  }

  @Test
  public void shouldResolveAliasedSelectStarByCallingParentIfParentHasNoSourceName() {
    // Given:
    when(source1.getSourceName()).thenReturn(Optional.empty());

    // When:
    final Stream<ColumnName> result = planNode.resolveSelectStar(Optional.of(SOURCE_2_NAME));

    // Then:
    final List<ColumnName> columns = result.collect(Collectors.toList());
    assertThat(columns, contains(COL0, COL1, COL2, COL3));

    verify(source1).resolveSelectStar(Optional.of(SOURCE_2_NAME));
    verify(source2).resolveSelectStar(Optional.of(SOURCE_2_NAME));
  }

  private final class TestPlanNode extends PlanNode {

    protected TestPlanNode(
        final PlanNodeId id,
        final DataSourceType nodeOutputType,
        final Optional<SourceName> sourceName
    ) {
      super(id, nodeOutputType, sourceName);
    }

    @Override
    public LogicalSchema getSchema() {
      return SCHEMA;
    }

    @Override
    public List<PlanNode> getSources() {
      return ImmutableList.of(source1, source2);
    }

    @Override
    protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
      return 0;
    }

    @Override
    public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
      return null;
    }
  }
}