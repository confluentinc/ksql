/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

@RunWith(MockitoJUnitRunner.class)
public class QueryStreamHandlerTest {

  private static final String QUERY_ID = "queryId";
  private static final List<String> COL_NAMES = ImmutableList.of(
      "A", "B", "C"
  );
  private static final List<String> COL_TYPES = ImmutableList.of(
      "INTEGER", "DOUBLE", "ARRAY"
  );
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("A"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("B"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("C"), SqlTypes.array(SqlTypes.STRING))
      .build();

  @Mock
  private Endpoints endpoints;
  @Mock
  private ConnectionQueryManager connectionQueryManager;
  @Mock
  private Context context;
  @Mock
  private Server server;
  @Mock
  private HttpServerRequest request;
  @Mock
  private HttpServerResponse response;
  @Mock
  private RoutingContext routingContext;
  @Mock
  private QueryPublisher publisher;
  @Mock
  private PushQueryHolder pushQueryHolder;
  @Mock
  private SocketAddress requestAddress;
  @Mock
  private PullQueryExecutorMetrics pullQueryMetrics;
  @Captor
  private ArgumentCaptor<Handler<Void>> endHandler;
  @Captor
  private ArgumentCaptor<Handler<Void>> closeHandler;
  @Captor
  private ArgumentCaptor<Subscriber<KeyValueMetadata<List<?>, GenericRow>>> subscriber;

  private QueryStreamHandler handler;


  // TODO Fix setup for print topic // complement tests
  @Before
  public void setUp() {
    when(routingContext.request()).thenReturn(request);
    when(routingContext.response()).thenReturn(response);
    when(request.version()).thenReturn(HttpVersion.HTTP_2);
    when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(request.remoteAddress()).thenReturn(SocketAddress.inetSocketAddress(9000, "remote"));
    CompletableFuture<Publisher<?>> future = new CompletableFuture<>();
    future.complete(publisher);
    when(endpoints.createQueryPublisher(any(), any(), any(), any(), any(), any(), any(), any(),
        any())).thenReturn(future);
    when(response.endHandler(endHandler.capture())).thenReturn(response);
    // The handler reads cancel-on-disconnect from config when constructed.
    when(server.getConfig()).thenReturn(new KsqlRestConfig(
        ImmutableMap.of(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")));
    when(publisher.queryId()).thenReturn(new QueryId(QUERY_ID));
    when(publisher.getColumnNames()).thenReturn(COL_NAMES);
    when(publisher.getColumnTypes()).thenReturn(COL_TYPES);
    when(publisher.geLogicalSchema()).thenReturn(SCHEMA);
    when(publisher.isPullQuery()).thenReturn(true);
    doNothing().when(publisher).subscribe(subscriber.capture());
    handler = new QueryStreamHandler(endpoints, connectionQueryManager, context, server, false);
  }

  private void givenRequest(final QueryStreamArgs req) {
    when(routingContext.getBody()).thenReturn(ServerUtils.serializeObject(req));
  }

  @Test
  public void shouldSucceed_pullQuery() {
    // Given:
    final QueryStreamArgs req = new QueryStreamArgs("select * from foo;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);

    // When:
    handler.handle(routingContext);
    endHandler.getValue().handle(null);

    // Then:
    assertThat(subscriber.getValue(), notNullValue());
    verify(publisher).close();
  }

  @Test
  public void shouldCancelAndCountWhenClientDisconnectsAndCancellationEnabled() {
    // Given: cancel-on-disconnect on, and a client that disconnects before the server completes.
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088",
        KsqlRestConfig.KSQL_QUERY_PULL_CANCEL_ON_DISCONNECT_ENABLED_CONFIG, true)));
    when(server.getPullQueryMetrics()).thenReturn(Optional.of(pullQueryMetrics));
    when(response.closeHandler(closeHandler.capture())).thenReturn(response);
    final QueryStreamHandler enabled =
        new QueryStreamHandler(endpoints, connectionQueryManager, context, server, false);
    givenRequest(new QueryStreamArgs("select * from foo;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));

    // When:
    enabled.handle(routingContext);
    closeHandler.getValue().handle(null);

    // Then: the disconnect is counted and the query is cancelled (publisher closed).
    verify(pullQueryMetrics).recordClientDisconnected();
    verify(publisher).close();
  }

  @Test
  public void shouldOnlyCountWhenClientDisconnectsAndCancellationDisabled() {
    // Given: the setUp handler has cancel-on-disconnect off (the default).
    when(server.getPullQueryMetrics()).thenReturn(Optional.of(pullQueryMetrics));
    when(response.closeHandler(closeHandler.capture())).thenReturn(response);
    givenRequest(new QueryStreamArgs("select * from foo;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));

    // When: the client disconnects before the server completes.
    handler.handle(routingContext);
    closeHandler.getValue().handle(null);

    // Then: the disconnect is counted but the query is NOT cancelled.
    verify(pullQueryMetrics).recordClientDisconnected();
    verify(publisher, never()).close();
  }

  @Test
  public void shouldNeitherCountNorCancelOnNormalServerCompletion() {
    // Given: cancel-on-disconnect on, but the server completes the query normally.
    // getPullQueryMetrics is deliberately not stubbed: the serverCompleted filter must return
    // before the handler ever reaches it.
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088",
        KsqlRestConfig.KSQL_QUERY_PULL_CANCEL_ON_DISCONNECT_ENABLED_CONFIG, true)));
    when(response.closeHandler(closeHandler.capture())).thenReturn(response);
    final QueryStreamHandler enabled =
        new QueryStreamHandler(endpoints, connectionQueryManager, context, server, false);
    givenRequest(new QueryStreamArgs("select * from foo;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
    enabled.handle(routingContext);

    // When: the subscriber marks the query server-completed, then the HTTP/2 stream closes.
    ((QuerySubscriber) subscriber.getValue()).handleComplete();
    closeHandler.getValue().handle(null);

    // Then: a normal stream close is neither counted as a disconnect nor cancelled.
    verify(pullQueryMetrics, never()).recordClientDisconnected();
    verify(publisher, never()).close();
  }

  @Test
  public void shouldSucceed_printQuery() {
    // Given:
    final QueryStreamArgs req = new QueryStreamArgs("print mytopic;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);

    // When:
    handler.handle(routingContext);
    endHandler.getValue().handle(null);

    // Then:
    assertThat(subscriber.getValue(), notNullValue());
    verify(publisher).close();
  }

  @Test
  public void shouldSucceed_pushQuery() {
    // Given:
    when(publisher.isPullQuery()).thenReturn(false);
    final QueryStreamArgs req = new QueryStreamArgs("select * from foo emit changes;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);
    when(connectionQueryManager.createApiQuery(any(), any())).thenReturn(pushQueryHolder);

    // When:
    handler.handle(routingContext);
    endHandler.getValue().handle(null);

    // Then:
    assertThat(subscriber.getValue(), notNullValue());
    verify(pushQueryHolder).close();
  }

  @Test
  public void shouldSucceed_scalablePushQuery() {
    // Given:
    when(publisher.isPullQuery()).thenReturn(false);
    when(publisher.isScalablePushQuery()).thenReturn(true);
    final QueryStreamArgs req = new QueryStreamArgs("select * from foo emit changes;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);

    // When:
    handler.handle(routingContext);
    endHandler.getValue().handle(null);

    // Then:
    assertThat(subscriber.getValue(), notNullValue());
    verify(publisher).close();
  }

  @Test
  public void verifyEndHandlerNotCalledTwice_scalablePushQuery() {
    // Given:
    when(publisher.isPullQuery()).thenReturn(false);
    when(publisher.isScalablePushQuery()).thenReturn(true);
    final QueryStreamArgs req = new QueryStreamArgs("select * from foo emit changes;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);

    // When:
    handler.handle(routingContext);
    endHandler.getValue().handle(null);
    endHandler.getValue().handle(null);

    // Then:
    assertThat(subscriber.getValue(), notNullValue());
    verify(publisher, times(1)).close();
  }

  @Test
  public void shouldNotCreatePublisherOnUnsupportedHttpVersion() {
    // Regression for the missing-return fix in handle(): without it, a request
    // on an HTTP version other than 1.1 or 2 was failed via routingContext.fail
    // but execution fell through to getRequest(...) and
    // endpoints.createQueryPublisher(...), starting a query asynchronously
    // against an already-failed routing context. The eventual response then
    // tripped "Response head already written" on the worker pool and the
    // started query was orphaned (no subscriber).
    when(request.version()).thenReturn(HttpVersion.HTTP_1_0);
    final QueryStreamArgs req = new QueryStreamArgs("select * from foo;",
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    givenRequest(req);

    // When:
    handler.handle(routingContext);

    // Then: no query publisher is created, and no subscription is set up.
    verify(endpoints, never()).createQueryPublisher(any(), any(), any(), any(), any(),
        any(), any(), any(), any());
    verify(publisher, never()).subscribe(any());
  }
}
