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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static io.confluent.ksql.util.KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator.PartitionLocation;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.vertx.core.streams.WriteStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
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
  private static final List<?> ROW3 = ImmutableList.of("e", "f");
  private static final List<?> ROW4 = ImmutableList.of("g", "h");

  private static final LogicalSchema SCHEMA = LogicalSchema.builder().build();
  private static final StreamedRow PQ_ROW1 = StreamedRow.pullRow(GenericRow.fromList(ROW1), Optional.empty());
  private static final StreamedRow PQ_ROW2 = StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty());
  private static final StreamedRow PQ_ROW3 = StreamedRow.pullRow(GenericRow.fromList(ROW3), Optional.empty());
  private static final StreamedRow PQ_ROW4 = StreamedRow.pullRow(GenericRow.fromList(ROW4), Optional.empty());

  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private RoutingOptions routingOptions;
  @Mock
  private QueryId queryId;
  @Mock
  private KsqlNode node1;
  @Mock
  private KsqlNode node2;
  @Mock
  private KsqlNode badNode;
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
  private KsqlConfig ksqlConfig;
  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private CompletableFuture<Void> disconnect;

  private KsqlPartitionLocation location1;
  private KsqlPartitionLocation location2;
  private KsqlPartitionLocation location3;
  private KsqlPartitionLocation location4;
  private KsqlPartitionLocation location5;

  private PullQueryWriteStream pullQueryQueue;

  @Mock
  private Time time;
  private static final String KSQL_SERVICE_ID = "harouting-test";
  private PullQueryExecutorMetrics pullMetrics;

  //@Mock
  private HARouting haRouting;

  @Before
  public void setUp() {
    when(pullPhysicalPlan.getMaterialization()).thenReturn(materialization);
    when(pullPhysicalPlan.getMaterialization().locator()).thenReturn(locator);
    when(statement.getUnMaskedStatementText()).thenReturn("foo");
    when(statement.getSessionConfig()).thenReturn(SessionConfig.of(ksqlConfig,
        ImmutableMap.of()));
    when(node1.isLocal()).thenReturn(true);
    when(node2.isLocal()).thenReturn(false);
    when(node1.location()).thenReturn(URI.create("http://node1:8088"));
    when(node2.location()).thenReturn(URI.create("http://node2:8089"));
    when(badNode.location()).thenReturn(URI.create("http://badnode:8090"));
    when(node1.getHost()).thenReturn(Host.include(new KsqlHostInfo("node1", 8088)));
    when(node2.getHost()).thenReturn(Host.include(new KsqlHostInfo("node2", 8089)));
    when(badNode.getHost()).thenReturn(Host.exclude(new KsqlHostInfo("badnode", 8090), "BAD"));

    location1 = new PartitionLocation(Optional.empty(), 1, ImmutableList.of(node1, node2));
    location2 = new PartitionLocation(Optional.empty(), 2, ImmutableList.of(node2, node1));
    location3 = new PartitionLocation(Optional.empty(), 3, ImmutableList.of(node1, node2));
    location4 = new PartitionLocation(Optional.empty(), 4, ImmutableList.of(node2, node1));
    location5 = new PartitionLocation(Optional.empty(), 4, ImmutableList.of(node2));
    // We require at least two threads, one for the orchestrator, and the other for the partitions.
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG))
        .thenReturn(1);
    when(ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG))
        .thenReturn(1);

    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);

    pullMetrics = new PullQueryExecutorMetrics(KSQL_SERVICE_ID, Collections.emptyMap(), time, new Metrics());

    haRouting = new HARouting(
        routingFilterFactory, Optional.of(pullMetrics), ksqlConfig);

    pullQueryQueue = new PullQueryWriteStream(
        OptionalInt.empty(),
        new StreamedRowTranslator(logicalSchema, Optional.empty())
    );
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
      final PullQueryWriteStream queue = i.getArgument(1);
      queue.write(ImmutableList.of(
          StreamedRow.header(queryId, logicalSchema),
          PQ_ROW1
      ));
      return null;
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    doNothing().when(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any())).thenAnswer(
      new Answer() {
        private int count = 0;

        public Object answer(InvocationOnMock i) {
          Map<String, ?> requestProperties = i.getArgument(3);
          PullQueryWriteStream rowConsumer = i.getArgument(4);
          if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("2")) {
            assertThat(count, is(0));
          }
          if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("4")) {
            assertThat(count, is(1));
            rowConsumer.write(
              ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
          }
          count++;
          return RestResponse.successful(200, 2);
        }
      }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    future.get();

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    verify(ksqlClient, times(2)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any());
    assertThat(pullQueryQueue.size(), is(2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(4.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void shouldCallRouteQuery_twoRound() throws InterruptedException, ExecutionException {
    // Given:
    locate(location1, location2, location3, location4);
    doAnswer(i -> {
      throw new StandbyFallbackException("Error!");
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    doNothing().when(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any())).thenAnswer(
        new Answer() {
          private int count = 0;

          public Object answer(InvocationOnMock i) {
            Map<String, ?> requestProperties = i.getArgument(3);
            WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("2")){
              assertThat(count, is(0));
              rowConsumer.write(
                ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
            }
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("4")){
              assertThat(count, is(1));
            }
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("1")){
              assertThat(count, is(2));
              rowConsumer.write(
                ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.pullRow(GenericRow.fromList(ROW1), Optional.empty())));
            }
            count++;

            return RestResponse.successful(200, 2);
          }
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan,
        statement, routingOptions, pullQueryQueue, disconnect, Optional.empty());
    future.get();

    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    verify(ksqlClient, times(3)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any());

    assertThat(pullQueryQueue.size(), is(2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(5.0));
    assertThat(resubmission_count, is(1.0));
  }

  @Test
  public void shouldCallRouteQuery_partitionFailure() throws InterruptedException, ExecutionException {
    // Given:
    locate(location1, location2, location3, location4);

    doThrow(new StandbyFallbackException("Error")).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    doAnswer(i -> {
      final PullQueryWriteStream queue = i.getArgument(1);
      queue.write(ImmutableList.of(PQ_ROW3));
      return null;
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());

    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any())).thenAnswer(
      new Answer() {
        private int count = 0;

        public Object answer(InvocationOnMock i) {
          Map<String, ?> requestProperties = i.getArgument(3);
          WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);

          if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("2")){
            assertThat(count, is(0));
            rowConsumer.write(
              ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
          }
          if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("4")){
            assertThat(count, is(1));
            rowConsumer.write(
              ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                StreamedRow.pullRow(GenericRow.fromList(ROW4), Optional.empty())));
          }
          if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("1")){
            assertThat(count, is(2));
            rowConsumer.write(
              ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                StreamedRow.pullRow(GenericRow.fromList(ROW1), Optional.empty())));
          }
          count++;

          return RestResponse.successful(200, 2);
        }
      }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan,
      statement, routingOptions, pullQueryQueue, disconnect, Optional.empty());
    future.get();

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    verify(ksqlClient, times(3)).makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any());

    assertThat(pullQueryQueue.size(), is(4));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW3));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW4));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(5.0));
    assertThat(resubmission_count, is(1.0));
  }

  @Test
  public void shouldCallRouteQuery_twoRound_networkError()
      throws InterruptedException, ExecutionException {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          throw new RuntimeException("Network error!");
        }
    );
    doAnswer(i -> {
      final PullQueryWriteStream queue = i.getArgument(1);
      queue.write(ImmutableList.of(StreamedRow.header(queryId, logicalSchema), PQ_ROW1));
      return null;
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location2.removeHeadHost())), any(), any());

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext, pullPhysicalPlan,
        statement, routingOptions, pullQueryQueue, disconnect, Optional.empty());
    future.get();

    // Then:
    verify(ksqlClient, times(1)).makeQueryRequest(eq(node2.location()), any(), any(), any(),
            any(), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location2.removeHeadHost())), any(), any());

    assertThat(pullQueryQueue.size(), is(1));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW1));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(2.0));
    assertThat(resubmission_count, is(1.0));
  }

  @Test
  public void shouldCallRouteQuery_allStandbysFail() {
    // Given:
    locate(location1, location2, location3, location4);
    doAnswer(i -> {
      throw new StandbyFallbackException("Error1!");
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    doAnswer(i -> {
      throw new StandbyFallbackException("Error1!");
    }).when(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any())).thenAnswer(
        new Answer() {
          private int count = 0;

          public Object answer(InvocationOnMock i) {
            Map<String, ?> requestProperties = i.getArgument(3);
            WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);

            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("2")){
              assertThat(count, is(0));
              rowConsumer.write(
                ImmutableList.of(StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));
            }
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("4")){
              assertThat(count, is(1));
            }
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("1")){
              throw new RuntimeException("Error2!");
            }
            if (requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS).toString().equalsIgnoreCase("3")){
              throw new RuntimeException("Error2!");
            }
            count++;

            return RestResponse.successful(200, 2);
          }
        }
    );

    // When:
    final Exception e = assertThrows(
        ExecutionException.class,
        () -> {
          CompletableFuture<Void> future = haRouting.handlePullQuery(serviceContext,
              pullPhysicalPlan, statement, routingOptions, pullQueryQueue,
              disconnect, Optional.empty());
          future.get();
        }
    );

    // Then:
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location1)), any(), any());
    verify(pullPhysicalPlan).execute(eq(ImmutableList.of(location3)), any(), any());
    verify(ksqlClient, atLeast(3)).makeQueryRequest(eq(node2.location()), any(), any(), any(),
            any(), any(), any());

    assertThat(e.getCause().getMessage(), containsString("Exhausted standby hosts to try."));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(6.0));
    assertThat(resubmission_count, is(2.0));
  }

  @Test
  public void shouldCallRouteQuery_couldNotFindHost() {
    // Given:
    location1 = new PartitionLocation(Optional.empty(), 1, ImmutableList.of());
    locate(location1, location2, location3, location4);

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement, routingOptions,
            pullQueryQueue, disconnect, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Unable to execute pull query. " +
            "[Partition 1 failed to find valid host. Hosts scanned: []]"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(0.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void shouldCallRouteQuery_allFilteredWithCause() {
    // Given:
    location1 = new PartitionLocation( Optional.empty(), 1, ImmutableList.of(badNode));
    locate(location1, location2, location3, location4);

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> haRouting.handlePullQuery(serviceContext, pullPhysicalPlan, statement, routingOptions,
            pullQueryQueue, disconnect, Optional.empty())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Hosts scanned: [badnode:8090 was not selected because BAD]]"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(0.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void shouldNotRouteToFilteredHost() throws InterruptedException, ExecutionException {
    // Given:
    location1 = new PartitionLocation(Optional.empty(), 1, ImmutableList.of(badNode, node1));
    when(ksqlClient.makeQueryRequest(any(), any(), any(), any(), any(), any(), any()))
        .then(invocationOnMock -> RestResponse.successful(200, 2));
    locate(location1, location2, location3, location4);

    // When:
    final CompletableFuture<Void> fut = haRouting.handlePullQuery(
        serviceContext,
        pullPhysicalPlan,
        statement,
        routingOptions,
        pullQueryQueue,
        disconnect,
        Optional.empty()
    );
    fut.get();

    // Then:
    verify(ksqlClient, never())
        .makeQueryRequest(eq(badNode.location()), any(), any(), any(), any(), any(), any());

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(4.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_errorRow() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.write(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.error(new RuntimeException("Row Error!"), 500)));
          return RestResponse.successful(200, 2);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(Throwables.getRootCause(e).getMessage(), containsString("Row Error!"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_authError() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.write(ImmutableList.of());
          return RestResponse.erroneous(401, "Authentication Error");
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(Throwables.getRootCause(e).getMessage(), containsString("Authentication Error"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_throwsError() {
    // Given:
    locate(location5);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Network Error"));

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(Throwables.getRootCause(e).getMessage(),
        containsString("Exhausted standby hosts to try."));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_cancelled() throws ExecutionException, InterruptedException {
    // Given:
    locate(location5);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(a -> {
          WriteStream<List<StreamedRow>> rowConsumer = a.getArgument(4);
          rowConsumer.write(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema),
                  StreamedRow.pullRow(GenericRow.fromList(ROW2), Optional.empty())));

          throw new RuntimeException("Cancelled");
        });
    when(disconnect.isDone()).thenReturn(true);

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    future.get();

    // Then:
    assertThat(pullQueryQueue.size(), is(1));
    assertThat(pullQueryQueue.pollRow(1, TimeUnit.SECONDS).getRow(), is(ROW2));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_noRows() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.write(ImmutableList.of());
          return RestResponse.successful(200, 0);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(Throwables.getRootCause(e).getMessage(),
        containsString("empty response from forwarding call, expected a header row"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  @Test
  public void forwardingError_invalidSchema() {
    // Given:
    locate(location2);
    when(ksqlClient.makeQueryRequest(eq(node2.location()), any(), any(), any(), any(), any(), any()))
        .thenAnswer(i -> {
          Map<String, ?> requestProperties = i.getArgument(3);
          WriteStream<List<StreamedRow>> rowConsumer = i.getArgument(4);
          assertThat(requestProperties.get(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS),
              is ("2"));
          rowConsumer.write(
              ImmutableList.of(
                  StreamedRow.header(queryId, logicalSchema2).withSourceHost(
                      new KsqlHostInfoEntity(
                          node2.location().getHost(), node2.location().getPort())),
                  StreamedRow.error(new RuntimeException("Row Error!"), 500)));
          return RestResponse.successful(200, 2);
        }
    );

    // When:
    CompletableFuture<Void> future = haRouting.handlePullQuery(
        serviceContext, pullPhysicalPlan, statement, routingOptions,
        pullQueryQueue, disconnect, Optional.empty());
    final Exception e = assertThrows(
        ExecutionException.class,
        future::get
    );

    // Then:
    assertThat(pullQueryQueue.size(), is(0));
    assertThat(Throwables.getRootCause(e).getMessage(),
        containsString("Schemas logicalSchema2 from host node2 differs from schema logicalSchema"));

    final double fetch_count = getMetricValue("-partition-fetch-count");
    final double resubmission_count = getMetricValue("-partition-fetch-resubmission-count");
    assertThat(fetch_count, is(1.0));
    assertThat(resubmission_count, is(0.0));
  }

  private void locate(final KsqlPartitionLocation... locations) {
    List<KsqlPartitionLocation> locationsList = ImmutableList.copyOf(locations);
    when(pullPhysicalPlan.getMaterialization().locator().locate(
        pullPhysicalPlan.getKeys(),
        routingOptions,
        routingFilterFactory,
        false
    )).thenReturn(locationsList);
  }

  private double getMetricValue(final String metricName) {
    final Metrics metrics = pullMetrics.getMetrics();
    final MetricName name = metrics.metricName("pull-query-requests" + metricName,
        "_confluent-ksql-pull-query",
        ImmutableMap.of(KSQL_SERVICE_ID_METRICS_TAG, KSQL_SERVICE_ID));
    final double val = (double) metrics.metric(name).metricValue();
    return val;
  }
}
