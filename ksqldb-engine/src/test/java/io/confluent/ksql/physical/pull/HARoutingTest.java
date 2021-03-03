/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.physical.pull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting.RouteQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class HARoutingTest {
  private static final List<?> ROW1 = ImmutableList.of("a", "b");
  private static final List<?> ROW2 = ImmutableList.of("c", "d");

  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private RoutingOptions routingOptions;
  @Mock
  private QueryId queryId;
  @Mock
  private KsqlPartitionLocation location1;
  @Mock
  private KsqlPartitionLocation location2;
  @Mock
  private KsqlPartitionLocation location3;
  @Mock
  private KsqlPartitionLocation location4;
  @Mock
  private KsqlNode node1;
  @Mock
  private KsqlNode node2;
  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private RoutingFilterFactory routingFilterFactory;
  @Mock
  private PullPhysicalPlan pullPhysicalPlan;
  @Mock
  private Materialization materialization;
  @Mock
  private Locator locator;
  @Mock
  private RouteQuery routeQuery;
  @Mock
  private KsqlConfig ksqlConfig;

  private HARouting haRouting;

  @Before
  public void setUp() {
    when(statement.getStatementText()).thenReturn("foo");
    when(location1.getNodes()).thenReturn(ImmutableList.of(node1, node2));
    when(location2.getNodes()).thenReturn(ImmutableList.of(node2, node1));
    when(location3.getNodes()).thenReturn(ImmutableList.of(node1, node2));
    when(location4.getNodes()).thenReturn(ImmutableList.of(node2, node1));
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG)).thenReturn(1);
    haRouting = new HARouting(
        routingFilterFactory, Optional.empty(), ksqlConfig, routeQuery);
  }

  @After
  public void tearDown() {
    if (haRouting != null) {
      haRouting.close();
    }
  }

  @Test
  public void shouldCallRouteQuery_success() throws InterruptedException {
    // Given:
    final PullQueryResult pullQueryResult1 = new PullQueryResult(
        ImmutableList.of(ROW1), Optional.empty(), logicalSchema, queryId);
    final PullQueryResult pullQueryResult2 = new PullQueryResult(
        ImmutableList.of(ROW2), Optional.empty(), logicalSchema, queryId);
    List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory
    )).thenReturn(locations);
    List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();
    when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          locationsQueried.add(inv.getArgument(1));
          return pullQueryResult1;
        });
    when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          locationsQueried.add(inv.getArgument(1));
          return pullQueryResult2;
        });

    // When:
    PullQueryResult result = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement,
        routingOptions, logicalSchema, queryId);

    // Then:
    verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(0).get(0), is(location1));
    assertThat(locationsQueried.get(0).get(1), is(location3));
    verify(routeQuery).routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(1).get(0), is(location2));
    assertThat(locationsQueried.get(1).get(1), is(location4));

    assertThat(result.getTableRows().size(), is(2));
    assertThat(result.getTableRows().get(0), is(ROW1));
    assertThat(result.getTableRows().get(1), is(ROW2));
  }

  @Test
  public void shouldCallRouteQuery_twoRound() throws InterruptedException {
    // Given:
    PullQueryResult pullQueryResult1 = new PullQueryResult(
        ImmutableList.of(ROW1), Optional.empty(), logicalSchema, queryId);
    PullQueryResult pullQueryResult2 = new PullQueryResult(
        ImmutableList.of(ROW2), Optional.empty(), logicalSchema, queryId);
    List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory
    )).thenReturn(locations);
    List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();
    when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          locationsQueried.add(inv.getArgument(1));
          throw new RuntimeException("Error!");
        });
    when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(new Answer() {
      private int count = 0;

      public Object answer(InvocationOnMock invocation) {
        locationsQueried.add(invocation.getArgument(1));
        if (++count == 1)
          return pullQueryResult2;

        return pullQueryResult1;
      }
    });

    // When:
    PullQueryResult result = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement,
        routingOptions, logicalSchema, queryId);

    // Then:
    verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(0).get(0), is(location1));
    assertThat(locationsQueried.get(0).get(1), is(location3));
    verify(routeQuery, times(2)).routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(1).get(0), is(location2));
    assertThat(locationsQueried.get(1).get(1), is(location4));
    assertThat(locationsQueried.get(2).get(0), is(location1));
    assertThat(locationsQueried.get(2).get(1), is(location3));

    assertThat(result.getTableRows().size(), is(2));
    assertThat(result.getTableRows().get(0), is(ROW2));
    assertThat(result.getTableRows().get(1), is(ROW1));
  }

  @Test
  public void shouldCallRouteQuery_allFail() {
    // Given:
    PullQueryResult pullQueryResult2 = new PullQueryResult(
        ImmutableList.of(ROW2), Optional.empty(), logicalSchema, queryId);
    List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory
    )).thenReturn(locations);
    List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();
    when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          locationsQueried.add(inv.getArgument(1));
          throw new RuntimeException("Error!");
        });
    when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(new Answer() {
          private int count = 0;

          public Object answer(InvocationOnMock invocation) {
            locationsQueried.add(invocation.getArgument(1));
            if (++count == 1)
              return pullQueryResult2;

            throw new RuntimeException("Error!");
          }
        });

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement, routingOptions,
            logicalSchema, queryId)
    );

    // Then:
    verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(0).get(0), is(location1));
    assertThat(locationsQueried.get(0).get(1), is(location3));
    verify(routeQuery, times(2)).routeQuery(eq(node2), any(), any(), any(), any(), any(), any(), any(), any());
    assertThat(locationsQueried.get(1).get(0), is(location2));
    assertThat(locationsQueried.get(1).get(1), is(location4));
    assertThat(locationsQueried.get(2).get(0), is(location1));
    assertThat(locationsQueried.get(2).get(1), is(location3));

    assertThat(e.getMessage(), containsString("Unable to execute pull query: foo. "
                                                  + "Exhausted standby hosts to try."));
  }

  @Test
  public void shouldCallRouteQuery_allFiltered() {
    // Given:
    when(location1.getNodes()).thenReturn(ImmutableList.of());
    List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory
    )).thenReturn(locations);

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement, routingOptions,
            logicalSchema, queryId)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Unable to execute pull query foo. All nodes are dead or exceed max allowed lag."));
  }
}
