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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting.RouteQuery;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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

  private static final LogicalSchema SCHEMA = LogicalSchema.builder().build();
  private static final PullQueryRow PQ_ROW1 = new PullQueryRow(ROW1, SCHEMA, Optional.empty());
  private static final PullQueryRow PQ_ROW2 = new PullQueryRow(ROW2, SCHEMA, Optional.empty());


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
  private LogicalSchema logicalSchema2;
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
  @Mock
  private SimpleKsqlClient ksqlClient;

  private PullQueryQueue pullQueryQueue = new PullQueryQueue();

  private HARouting haRouting;

  @Before
  public void setUp() {
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(statement.getStatementText()).thenReturn("foo");
    when(statement.getSessionConfig()).thenReturn(SessionConfig.of(ksqlConfig,
        ImmutableMap.of()));
    when(node1.isLocal()).thenReturn(true);
    when(node2.isLocal()).thenReturn(false);
    when(node1.location()).thenReturn(URI.create("http://node1:8088"));
    when(node2.location()).thenReturn(URI.create("http://node2:8089"));
    when(location1.getNodes()).thenReturn(ImmutableList.of(node1, node2));
    when(location2.getNodes()).thenReturn(ImmutableList.of(node2, node1));
    when(location3.getNodes()).thenReturn(ImmutableList.of(node1, node2));
    when(location4.getNodes()).thenReturn(ImmutableList.of(node2, node1));
    when(location1.getPartition()).thenReturn(1);
    when(location2.getPartition()).thenReturn(2);
    when(location3.getPartition()).thenReturn(3);
    when(location4.getPartition()).thenReturn(4);
    // We require at least two threads, one for the orchestrator, and the other for the partitions.
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG)).thenReturn(2);

    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);
    haRouting = new HARouting(
        routingFilterFactory, Optional.empty(), ksqlConfig);
  }

  @After
  public void tearDown() {
    if (haRouting != null) {
      haRouting.close();
    }
  }

  @Test
  public void shouldCallRouteQuery_success() throws InterruptedException, ExecutionException {
    // Given:
    locate(location1, location2, location3, location4);
    doAnswer(i -> {
      final PullQueryQueue queue = i.getArgument(1);
      queue.acceptRow(PQ_ROW1);
      return null;
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2,4"));
          rowConsumer.accept(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
          return RestResponse.successful(200, 2);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId,
        pullQueryQueue);
    future.get();

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());

    assertThat(pullQueryQueue.size(), is(2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));
  }

  @Test
  public void shouldCallRouteQuery_twoRound() throws InterruptedException, ExecutionException {
    // Given:
    locate(location1, location2, location3, location4);
    doAnswer(i -> {
      throw new StandbyFallbackException("Error!");
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        new Answer() {
          private int count = 0;

          public Object answer(InvocationOnMock i) {
            Map<String, ?> requestProperties = i.getArgument(3);
            Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
            if (++count == 1) {
              assertThat(requestProperties.get(
                  KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS), is ("2,4"));
              rowConsumer.accept(
                  ImmutableList.of(
                      StreamedRow.header(queryId, logicalSchema),
                      StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
            } else {
              assertThat(requestProperties.get(
                  KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS), is ("1,3"));
              rowConsumer.accept(
                  ImmutableList.of(
                      StreamedRow.header(queryId, logicalSchema),
                      StreamedRow.pullRow(GenericRow.fromList(ROW1), Optional.empty())));
            }

            return RestResponse.successful(200, 2);
          }
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan,
        statement, routingOptions, logicalSchema, queryId, pullQueryQueue);
    future.get();

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());
    verify(ksqlClient, times(2)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any());

    assertThat(pullQueryQueue.size(), is(2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));
  }

  @Test
  public void shouldCallRouteQuery_twoRound_networkError()
      throws InterruptedException, ExecutionException {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          throw new RuntimeException("Network error!");
        }
    );
    doAnswer(i -> {
      final PullQueryQueue queue = i.getArgument(1);
      queue.acceptRow(PQ_ROW1);
      return null;
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location2)), any(), any());

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan,
        statement, routingOptions, logicalSchema, queryId, pullQueryQueue);
    future.get();

    // Then:
    verify(ksqlClient, times(1)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location2)), any(), any());

    assertThat(pullQueryQueue.size(), is(1));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));
  }

  @Test
  public void shouldCallRouteQuery_allStandbysFail() {
    // Given:
    locate(location1, location2, location3, location4);
    doAnswer(i -> {
      throw new StandbyFallbackException("Error1!");
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        new Answer() {
          private int count = 0;

          public Object answer(InvocationOnMock i) {
            Map<String, ?> requestProperties = i.getArgument(3);
            Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
            if (++count == 1) {
              assertThat(requestProperties.get(
                  KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS), is ("2,4"));
              rowConsumer.accept(
                  ImmutableList.of(
                      StreamedRow.header(queryId, logicalSchema),
                      StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
            } else {
              assertThat(requestProperties.get(
                  KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS), is ("1,3"));
              throw new RuntimeException("Error2!");
            }

            return RestResponse.successful(200, 2);
          }
        }
    );

    // When:
    final Exception e = assertThrows(
        ExecutionException.class,
        () -> {
          CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext,
              pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId, pullQueryQueue);
          future.get();
        }
    );

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1, location3)), any(), any());
    verify(ksqlClient, times(2)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any());

    assertThat(e.getCause().getMessage(), containsString("Unable to execute pull query: \"foo\". "
                                                  + "Exhausted standby hosts to try."));
  }

  @Test
  public void shouldCallRouteQuery_allFiltered() {
    // Given:
    when(location1.getNodes()).thenReturn(ImmutableList.of());
    locate(location1, location2, location3, location4);

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement, routingOptions,
            logicalSchema, queryId, pullQueryQueue)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Unable to execute pull query \"foo\". All nodes are dead or exceed max allowed lag."));
  }

  @Test
  public void forwardingError_errorRow() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.accept(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.error(new RuntimeException("Row Error!"), 500)));
          return RestResponse.successful(200, 2);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId,
        pullQueryQueue);
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(e.getMessage(), containsString("Row Error!"));
  }

  @Test
  public void forwardingError_authError() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.accept(ImmutableList.of());
          return RestResponse.erroneous(401, "Authentication Error");
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId,
        pullQueryQueue);
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(e.getMessage(), containsString("Authentication Error"));
  }

  @Test
  public void forwardingError_noRows() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.accept(ImmutableList.of());
          return RestResponse.successful(200, 0);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId,
        pullQueryQueue);
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(e.getMessage(),
        containsString("empty response from forwarding call, expected a header row"));
  }

  @Test
  public void forwardingError_invalidSchema() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any())).thenAnswer(
        i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          Consumer<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.accept(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema2),
                  StreamedRow.error(new RuntimeException("Row Error!"), 500)));
          return RestResponse.successful(200, 2);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions, logicalSchema, queryId,
        pullQueryQueue);
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(e.getMessage(),
        containsString("Schemas logicalSchema2 from host node2 differs from schema logicalSchema"));
  }

  private void locate(final KsqlPartitionLocation... locations) {
    List<KsqlPartitionLocation> locationsList = ImmutableList.copyOf(locations);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory
    )).thenReturn(locationsList);
  }
}
