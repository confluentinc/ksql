/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.query.QueryMetadataHolder;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Regression tests for KSQL-15211: the {@code /query-stream} pull-query path must run the
 * same per-user RBAC {@link KsqlAuthorizationValidator} check that the legacy {@code /query}
 * path (StreamedQueryResource) and websocket path (WSQueryEndpoint) already enforce, before
 * ever reading materialized state.
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryEndpointTest {

  private static final String TOPIC_NAME = "test_stream";
  private static final String PULL_QUERY_STRING =
      "SELECT * FROM " + TOPIC_NAME + " WHERE ROWKEY='null';";

  private static Vertx vertx;
  private static WorkerExecutor workerExecutor;

  @Mock
  private KsqlExecutionContext ksqlEngine;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private QueryExecutor queryExecutor;
  @Mock
  private QueryMetadataHolder queryMetadataHolder;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private Context context;

  private KsqlSecurityContext securityContext;
  private QueryEndpoint queryEndpoint;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
    workerExecutor = vertx.createSharedWorkerExecutor("query-endpoint-test-worker");
  }

  @AfterClass
  public static void tearDownClass() {
    workerExecutor.close();
    vertx.close();
  }

  @Before
  public void setUp() {
    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    final ParsedStatement parsedStatement = mock(ParsedStatement.class);
    when(ksqlEngine.parse(PULL_QUERY_STRING))
        .thenReturn(ImmutableList.of(parsedStatement));
    when(ksqlEngine.prepare(any(), any()))
        .thenAnswer(invocation -> PreparedStatement.of(PULL_QUERY_STRING, mock(Query.class)));

    when(queryExecutor.handleStatement(
        any(), any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(queryMetadataHolder);
    when(queryMetadataHolder.getPullQueryResult()).thenReturn(Optional.empty());
    when(queryMetadataHolder.getPushQueryMetadata()).thenReturn(Optional.empty());

    queryEndpoint = new QueryEndpoint(
        ksqlEngine,
        ksqlConfig,
        Optional.empty(),
        queryExecutor,
        Optional.of(authorizationValidator));
  }

  @Test
  public void shouldCheckAuthorizationBeforeExecutingPullQuery() {
    // When: (the statement executes - no source topic ACLs are denied)
    invokeCreateQueryPublisher(PULL_QUERY_STRING);

    // Then: the per-user RBAC check ran, and the statement was handed to the executor
    verify(authorizationValidator).checkAuthorization(
        eq(securityContext), eq(metaStore), any(Statement.class));
    verify(queryExecutor).handleStatement(
        any(), any(), any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void shouldDenyPullQueryWhenAuthorizationValidatorRejects() {
    // Given: alice has no READ grant on the source topic
    doThrow(new KsqlTopicAuthorizationException(AclOperation.READ,
        Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Throwable thrown = invokeCreateQueryPublisherExpectingFailure(PULL_QUERY_STRING);

    // Then: the request is denied, and no materialized state is ever read
    assertThat(thrown, instanceOf(KsqlTopicAuthorizationException.class));
    verify(queryExecutor, never()).handleStatement(
        any(), any(), any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void shouldSkipAuthorizationCheckWhenValidatorNotConfigured() {
    // Given: no authorization validator configured, e.g. OSS with no security extension
    queryEndpoint = new QueryEndpoint(
        ksqlEngine,
        ksqlConfig,
        Optional.empty(),
        queryExecutor,
        Optional.empty());

    // When:
    invokeCreateQueryPublisher(PULL_QUERY_STRING);

    // Then: the query still executes, since there's no validator to enforce
    verify(queryExecutor).handleStatement(
        any(), any(), any(), any(), any(), any(), any(), anyBoolean());
  }

  private void invokeCreateQueryPublisher(final String sql) {
    // The statement reaches the executor, but the metadata holder has neither a pull nor push
    // result configured, so a KsqlStatementException is expected past the authorization check;
    // that's fine here, since these tests only assert on what happens up to and including
    // dispatch to the executor.
    invokeCreateQueryPublisherExpectingFailure(sql);
  }

  private Throwable invokeCreateQueryPublisherExpectingFailure(final String sql) {
    final CompletableFuture<Throwable> future = new CompletableFuture<>();
    workerExecutor.<Throwable>executeBlocking(promise -> {
      try {
        queryEndpoint.createQueryPublisher(
            sql,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            context,
            workerExecutor,
            securityContext,
            new MetricsCallbackHolder(),
            Optional.empty());
        promise.complete(null);
      } catch (final Throwable t) {
        promise.complete(t);
      }
    }, false, ar -> future.complete(ar.result()));

    try {
      return future.get(10, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new AssertionError("Timed out waiting for worker thread", e);
    }
  }
}
