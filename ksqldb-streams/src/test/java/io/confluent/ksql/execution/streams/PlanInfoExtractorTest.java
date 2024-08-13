/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (final the "License"); you may not use
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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.PlanInfoExtractor;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PlanInfoExtractorTest {

  @Mock
  private QueryContext queryContext;
  @Mock
  private LogicalSchema schema;
  @Mock
  private Formats formats;
  @Mock
  private ColumnName joinKey;
  @Mock
  private Expression repartitionKey;

  private StreamSource streamSource;
  private TableSource tableSource;
  private StreamSelectKey<GenericKey> streamSourceRepartitioned;
  private StreamTableJoin<GenericKey> streamAndTableJoined;
  private StreamTableJoin<GenericKey> streamRepartitionedAndTableJoined;
  private StreamSelectKey<GenericKey> streamAndTableJoinedRepartitioned;

  private PlanInfoExtractor planInfoExtractor;

  @Before
  public void setUp() {
    streamSource = new StreamSource(
        new ExecutionStepPropertiesV1(queryContext),
        "s1",
        formats,
        Optional.empty(),
        schema,
        OptionalInt.of(SystemColumns.CURRENT_PSEUDOCOLUMN_VERSION_NUMBER)
    );
    tableSource = new TableSource(
        new ExecutionStepPropertiesV1(queryContext),
        "t1",
        formats,
        Optional.empty(),
        schema,
        SystemColumns.CURRENT_PSEUDOCOLUMN_VERSION_NUMBER,
        formats
    );
    streamSourceRepartitioned = new StreamSelectKey<>(
        new ExecutionStepPropertiesV1(queryContext),
        streamSource,
        ImmutableList.of(repartitionKey)
    );
    streamAndTableJoined = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        JoinType.LEFT,
        joinKey,
        formats,
        streamSource,
        tableSource
    );
    streamRepartitionedAndTableJoined = new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        JoinType.LEFT,
        joinKey,
        formats,
        streamSourceRepartitioned,
        tableSource
    );
    streamAndTableJoinedRepartitioned = new StreamSelectKey<>(
        new ExecutionStepPropertiesV1(queryContext),
        streamAndTableJoined,
        ImmutableList.of(repartitionKey)
    );

    planInfoExtractor = new PlanInfoExtractor();
  }

  @Test
  public void shouldExtractSource() {
    // When:
    final PlanInfo planInfo = streamSource.extractPlanInfo(planInfoExtractor);

    // Then:
    assertThat(planInfo.isRepartitionedInPlan(streamSource), is(false));
  }

  @Test
  public void shouldExtractSourceWithRepartition() {
    // When:
    final PlanInfo planInfo = streamSourceRepartitioned.extractPlanInfo(planInfoExtractor);

    // Then:
    assertThat(planInfo.isRepartitionedInPlan(streamSource), is(true));
  }

  @Test
  public void shouldExtractMultipleSources() {
    // When:
    final PlanInfo planInfo = streamAndTableJoined.extractPlanInfo(planInfoExtractor);

    // Then:
    assertThat(planInfo.isRepartitionedInPlan(streamSource), is(false));
    assertThat(planInfo.isRepartitionedInPlan(tableSource), is(false));
  }

  @Test
  public void shouldExtractRepartitionBeforeJoin() {
    // When:
    final PlanInfo planInfo = streamRepartitionedAndTableJoined.extractPlanInfo(planInfoExtractor);

    // Then:
    assertThat(planInfo.isRepartitionedInPlan(streamSource), is(true));
    assertThat(planInfo.isRepartitionedInPlan(tableSource), is(false));
  }

  @Test
  public void shouldExtractRepartitionAfterJoin() {
    // When:
    final PlanInfo planInfo = streamAndTableJoinedRepartitioned.extractPlanInfo(planInfoExtractor);

    // Then:
    assertThat(planInfo.isRepartitionedInPlan(streamSource), is(false));
    assertThat(planInfo.isRepartitionedInPlan(tableSource), is(false));
  }
}