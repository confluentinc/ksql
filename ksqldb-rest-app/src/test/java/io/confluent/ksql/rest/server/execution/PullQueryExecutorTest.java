/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers.statement;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatementErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.TableRows;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor.PullQueryContext;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor.RouteQuery;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class PullQueryExecutorTest {
  private static final RoutingFilterFactory ROUTING_FILTER_FACTORY =
      (routingOptions, hosts, active, applicationQueryId, storeName, partition) ->
          new RoutingFilters(ImmutableList.of());

  public static class Disabled {
    @Rule
    public final TemporaryEngine engine = new TemporaryEngine()
        .withConfigs(ImmutableMap.of(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG, false));

    @Test
    public void shouldThrowExceptionIfConfigDisabled() {
      // Given:
      final Query theQuery = mock(Query.class);
      when(theQuery.isPullQuery()).thenReturn(true);
      final ConfiguredStatement<Query> query = ConfiguredStatement
          .of(PreparedStatement.of("SELECT * FROM test_table;", theQuery),
              SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of()));
      PullQueryExecutor pullQueryExecutor = new PullQueryExecutor(
          engine.getEngine(), ROUTING_FILTER_FACTORY, engine.getKsqlConfig()
      );

      // When:
      final Exception e = assertThrows(
          KsqlStatementException.class,
          () -> pullQueryExecutor.execute(
              query, ImmutableMap.of(), engine.getServiceContext(), Optional.empty(),  Optional.empty())
      );

      // Then:
      assertThat(e.getMessage(), containsString(
          "Pull queries are disabled"));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class Enabled {

    @Rule
    public final TemporaryEngine engine = new TemporaryEngine();

    @Test
    public void shouldRedirectQueriesToQueryEndPoint() {
      // Given:
      final ConfiguredStatement<Query> query = ConfiguredStatement
          .of(PreparedStatement.of("SELECT * FROM test_table;", mock(Query.class)),
              SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of()));

      // When:
      final KsqlRestException e = assertThrows(
          KsqlRestException.class,
          () -> CustomValidators.QUERY_ENDPOINT.validate(
              query,
              mock(SessionProperties.class),
              engine.getEngine(),
              engine.getServiceContext()
          )
      );

      // Then:
      assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
      assertThat(e, exceptionStatementErrorMessage(errorMessage(containsString(
          "The following statement types should be issued to the websocket endpoint '/query'"
      ))));
      assertThat(e, exceptionStatementErrorMessage(statement(containsString(
          "SELECT * FROM test_table;"))));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class RateLimit {

    @Rule
    public final TemporaryEngine engine = new TemporaryEngine()
        .withConfigs(ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG, 2));

    @Mock
    private Time time;

    @Test
    public void shouldRateLimit() {
      PullQueryExecutor pullQueryExecutor = new PullQueryExecutor(
          engine.getEngine(), ROUTING_FILTER_FACTORY, engine.getKsqlConfig()
      );

      // When:
      pullQueryExecutor.checkRateLimit();
      assertThrows(KsqlException.class, pullQueryExecutor::checkRateLimit);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class UnitTests {
    private static final List<?> ROW1 = ImmutableList.of("a", "b");
    private static final List<?> ROW2 = ImmutableList.of("c", "d");

    @Mock
    private ConfiguredStatement<Query> statement;
    @Mock
    private KsqlExecutionContext executionContext;
    @Mock
    private ServiceContext serviceContext;
    @Mock
    private RoutingOptions routingOptions;
    @Mock
    private PullQueryContext pullQueryContext;
    @Mock
    private QueryId queryId;
    @Mock
    private RouteQuery routeQuery;
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
    private ExecutorService executorService;
    @Mock
    private RoutingFilterFactory routingFilterFactory;

    @Before
    public void setUp() {
      when(statement.getMaskedStatementText()).thenReturn("foo");
      when(location1.getNodes()).thenReturn(ImmutableList.of(node1, node2));
      when(location2.getNodes()).thenReturn(ImmutableList.of(node2, node1));
      when(location3.getNodes()).thenReturn(ImmutableList.of(node1, node2));
      when(location4.getNodes()).thenReturn(ImmutableList.of(node2, node1));
      when(logicalSchema.key()).thenReturn(ImmutableList.of(Column.of(ColumnName.of("ID"),
          SqlTypes.STRING, Namespace.KEY, 0)));
      when(logicalSchema.value()).thenReturn(ImmutableList.of(Column.of(ColumnName.of("VAL"),
          SqlTypes.STRING, Namespace.VALUE, 1)));
    }

    @Test
    public void shouldCallRouteQuery_success() throws InterruptedException {
      TableRows rows = new TableRows("", queryId, logicalSchema, ImmutableList.of(ROW1));
      when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any())).thenReturn(rows);
      rows = new TableRows("", queryId, logicalSchema, ImmutableList.of(ROW2));
      when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any())).thenReturn(rows);
      List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
      List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();
      PullQueryResult result = PullQueryExecutor.handlePullQuery(
          statement, executionContext, serviceContext, routingOptions, (l) -> {
            locationsQueried.add(l);
            return pullQueryContext;
          }, queryId, locations, Executors.newSingleThreadExecutor(), routeQuery);
      verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any());
      assertThat(locationsQueried.get(0).get(0), is(location1));
      assertThat(locationsQueried.get(0).get(1), is(location3));
      verify(routeQuery).routeQuery(eq(node2), any(), any(), any(), any());
      assertThat(locationsQueried.get(1).get(0), is(location2));
      assertThat(locationsQueried.get(1).get(1), is(location4));

      assertThat(result.getTableRows().getRows().size(), is(2));
      assertThat(result.getTableRows().getRows().get(0), is(ROW1));
      assertThat(result.getTableRows().getRows().get(1), is(ROW2));
    }

    @Test
    public void shouldCallRouteQuery_twoRound() throws InterruptedException {
      when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any()))
          .thenThrow(new RuntimeException("Error!"));
      TableRows rows1 = new TableRows("", queryId, logicalSchema, ImmutableList.of(ROW1));
      TableRows rows2 = new TableRows("", queryId, logicalSchema, ImmutableList.of(ROW2));
      when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any()))
          .thenReturn(rows2)
          .thenReturn(rows1);
      List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
      List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();
      PullQueryResult result = PullQueryExecutor.handlePullQuery(
          statement, executionContext, serviceContext, routingOptions, (l) -> {
            locationsQueried.add(l);
            return pullQueryContext;
          }, queryId, locations, Executors.newSingleThreadExecutor(), routeQuery);
      verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any());
      assertThat(locationsQueried.get(0).get(0), is(location1));
      assertThat(locationsQueried.get(0).get(1), is(location3));
      verify(routeQuery, times(2)).routeQuery(eq(node2), any(), any(), any(), any());
      assertThat(locationsQueried.get(1).get(0), is(location2));
      assertThat(locationsQueried.get(1).get(1), is(location4));
      assertThat(locationsQueried.get(2).get(0), is(location1));
      assertThat(locationsQueried.get(2).get(1), is(location3));

      assertThat(result.getTableRows().getRows().size(), is(2));
      assertThat(result.getTableRows().getRows().get(0), is(ROW2));
      assertThat(result.getTableRows().getRows().get(1), is(ROW1));
    }

    @Test
    public void shouldCallRouteQuery_allFail() {
      when(routeQuery.routeQuery(eq(node1), any(), any(), any(), any()))
          .thenThrow(new RuntimeException("Error!"));
      TableRows rows2 = new TableRows("", queryId, logicalSchema, ImmutableList.of(ROW2));
      when(routeQuery.routeQuery(eq(node2), any(), any(), any(), any()))
          .thenReturn(rows2)
          .thenThrow(new RuntimeException("Error!"));
      List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
      List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();

      final Exception e = assertThrows(
          MaterializationException.class,
          () -> PullQueryExecutor.handlePullQuery(
              statement, executionContext, serviceContext, routingOptions, (l) -> {
                locationsQueried.add(l);
                return pullQueryContext;
              }, queryId, locations, Executors.newSingleThreadExecutor(), routeQuery)
      );

      verify(routeQuery).routeQuery(eq(node1), any(), any(), any(), any());
      assertThat(locationsQueried.get(0).get(0), is(location1));
      assertThat(locationsQueried.get(0).get(1), is(location3));
      verify(routeQuery, times(2)).routeQuery(eq(node2), any(), any(), any(), any());
      assertThat(locationsQueried.get(1).get(0), is(location2));
      assertThat(locationsQueried.get(1).get(1), is(location4));
      assertThat(locationsQueried.get(2).get(0), is(location1));
      assertThat(locationsQueried.get(2).get(1), is(location3));

      assertThat(e.getMessage(), containsString("Unable to execute pull query: foo. "
          + "Exhausted standby hosts to try."));
    }

    @Test
    public void shouldCallRouteQuery_allFiltered() {
      when(location1.getNodes()).thenReturn(ImmutableList.of());
      List<KsqlPartitionLocation> locations = ImmutableList.of(location1, location2, location3, location4);
      List<List<KsqlPartitionLocation>> locationsQueried = new ArrayList<>();

      final Exception e = assertThrows(
          MaterializationException.class,
          () -> PullQueryExecutor.handlePullQuery(
              statement, executionContext, serviceContext, routingOptions, (l) -> {
                locationsQueried.add(l);
                return pullQueryContext;
              }, queryId, locations, Executors.newSingleThreadExecutor(), routeQuery)
      );

      assertThat(e.getMessage(), containsString("Unable to execute pull query foo. "
          + "All nodes are dead or exceed max allowed lag."));
    }

    @Test
    public void shouldCloseExecutorOnClose() throws Exception {
      // Given:
      final PullQueryExecutor executor =
          new PullQueryExecutor(executionContext, routingFilterFactory, 10, executorService);

      // When:
      executor.close(Duration.ofSeconds(30));

      // Then:
      verify(executorService).shutdown();
      verify(executorService).awaitTermination(30_000, TimeUnit.MILLISECONDS);
    }
  }
}
