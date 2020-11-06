/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.PlanNodeId;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryIdUtilTest {

  private static final SourceName SINK = SourceName.of("SINK");

  @Mock
  private KsqlBareOutputNode transientPlan;
  @Mock
  private KsqlStructuredDataOutputNode plan;
  @Mock
  private QueryIdGenerator idGenerator;
  @Mock
  private EngineContext engineContext;

  @Test
  public void shouldGenerateUniqueRandomIdsForTransientQueries() {
    // Given:
    when(transientPlan.getSinkName()).thenReturn(Optional.empty());

    // When:
    long numUniqueIds = IntStream.range(0, 100)
        .mapToObj(i -> QueryIdUtil.buildId(engineContext, idGenerator, transientPlan,
            false, Optional.empty()))
        .distinct()
        .count();

    // Then:
    assertThat(numUniqueIds, is(100L));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForInsertInto() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(idGenerator.getNext()).thenReturn("1");

    // When:
    final QueryId queryId = QueryIdUtil.buildId(engineContext, idGenerator, plan,
        false, Optional.empty());

    // Then:
    assertThat(queryId, is(new QueryId("INSERTQUERY_1")));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForNewStream() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(plan.getId()).thenReturn(new PlanNodeId("FOO"));
    when(plan.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(plan.createInto()).thenReturn(true);
    when(idGenerator.getNext()).thenReturn("1");
    when(engineContext.getQueriesWithSink(SINK)).thenReturn(ImmutableSet.of());

    // When:
    final QueryId queryId = QueryIdUtil.buildId(engineContext, idGenerator, plan,
        false, Optional.empty());
    // Then:
    assertThat(queryId, is(new QueryId("CSAS_FOO_1")));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForNewTable() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(plan.getId()).thenReturn(new PlanNodeId("FOO"));
    when(plan.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    when(plan.createInto()).thenReturn(true);
    when(idGenerator.getNext()).thenReturn("1");
    when(engineContext.getQueriesWithSink(SINK)).thenReturn(ImmutableSet.of());

    // When:
    final QueryId queryId = QueryIdUtil.buildId(engineContext, idGenerator, plan,
        false, Optional.empty());

    // Then:
    assertThat(queryId, is(new QueryId("CTAS_FOO_1")));
  }

  @Test
  public void shouldReuseExistingQueryId() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(plan.createInto()).thenReturn(true);
    when(engineContext.getQueriesWithSink(SINK))
        .thenReturn(ImmutableSet.of(new QueryId("CTAS_FOO_10")));

    // When:
    final QueryId queryId = QueryIdUtil.buildId(engineContext, idGenerator, plan,
        true, Optional.empty());

    // Then:
    assertThat(queryId, is(new QueryId("CTAS_FOO_10")));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnReuseIfCreateOrReplacedIsDisabled() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(plan.createInto()).thenReturn(true);
    when(plan.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(engineContext.getQueriesWithSink(SINK))
        .thenReturn(ImmutableSet.of(new QueryId("CTAS_FOO_10")));

    // When:
    QueryIdUtil.buildId(engineContext, idGenerator, plan,
        false, Optional.empty());
  }

  @Test
  public void shouldThrowIfMultipleQueriesExist() {
    // Given:
    when(plan.getSinkName()).thenReturn(Optional.of(SINK));
    when(plan.createInto()).thenReturn(true);
    when(engineContext.getQueriesWithSink(SINK))
        .thenReturn(ImmutableSet.of(new QueryId("CTAS_FOO_1"), new QueryId("INSERTQUERY_1")));

    // When:
    final KsqlException e = assertThrows(KsqlException.class, () ->
        QueryIdUtil.buildId(engineContext, idGenerator, plan, false, Optional.empty()));

    // Then:
    assertThat(e.getMessage(), containsString("there are multiple queries writing"));
  }

  @Test
  public void shouldReturnWithQueryIdInUppercase(){
    // When:
    final QueryId queryId = QueryIdUtil.buildId(engineContext, idGenerator, plan,
        false, Optional.of("my_query_id"));

    // Then:
    assertThat(queryId, is(new QueryId("MY_QUERY_ID")));
  }

  @Test
  public void shouldThrowIfWithQueryIdIsReserved() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> QueryIdUtil.buildId(engineContext, idGenerator, plan,
            false, Optional.of("insertquery_custom"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Query IDs must not start with a "
        + "reserved query ID prefix (INSERTQUERY_, CTAS_, CSAS_). Got 'INSERTQUERY_CUSTOM'."));
  }

  @Test
  public void shouldThrowIfWithQueryIdIsNotValid() {
    // When:
    final Exception e = assertThrows(
        Exception.class,
        () -> QueryIdUtil.buildId(engineContext, idGenerator, plan,
            false, Optional.of("with space"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Query IDs may contain only alphanumeric characters and '_'. Got: 'WITH SPACE'"));
  }
}