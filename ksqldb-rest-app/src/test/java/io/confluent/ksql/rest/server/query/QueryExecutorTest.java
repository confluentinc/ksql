package io.confluent.ksql.rest.server.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.execution.pull.HARouting;
import io.confluent.ksql.execution.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.scalablepush.PushRouting;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.rest.util.RateLimiter;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryExecutorTest {

  private static final String TOPIC_NAME = "test_stream";
  private static final String PUSH_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " EMIT CHANGES;";
  private static final String PULL_QUERY_STRING =
      "SELECT * FROM " + TOPIC_NAME + " WHERE ROWKEY='null';";
  private static final long ROWS_RETURNED = 10;
  private static final long ROWS_PROCESSED = 15;


  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConcurrencyLimiter concurrencyLimiter;
  @Mock
  private SlidingWindowRateLimiter pullBandRateLimiter;
  @Mock
  private SlidingWindowRateLimiter scalablePushBandRateLimiter;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlRestConfig ksqlRestConfig;
  @Mock
  private PullQueryExecutorMetrics pullQueryExecutorMetrics;
  @Mock
  private ScalablePushQueryMetrics scalablePushQueryMetrics;
  @Mock
  private PullQueryResult pullQueryResult;
  @Mock
  private HARouting haRouting;
  @Mock
  private PushRouting pushRouting;
  @Mock
  private LocalCommands localCommands;
  @Mock
  private Context context;
  @Mock
  private ImmutableAnalysis mockAnalysis;
  @Mock
  private AliasedDataSource mockAliasedDataSource;
  @Mock
  private DataSource mockDataSource;
  @Mock
  private StreamPullQueryMetadata streamPullQueryMetadata;
  @Mock
  private TransientQueryMetadata transientQueryMetadata;
  @Mock
  private ScalablePushQueryMetadata scalablePushQueryMetadata;
  @Mock
  private Decrementer decrementer;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsqlTopic ksqlTopic;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private TransientQueryQueue transientQueryQueue;

  private RateLimiter rateLimiter;
  private PreparedStatement<Query> pullQuery;
  private PreparedStatement<Query> pushQuery;
  private MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
  private QueryExecutor queryExecutor;

  @Before
  public void setUp() {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)).thenReturn(true);
    when(ksqlRestConfig.getInt(KsqlRestConfig.MAX_PUSH_QUERIES)).thenReturn(Integer.MAX_VALUE);
    when(ksqlEngine.analyzeQueryWithNoOutputTopic(any(), any(), any())).thenReturn(mockAnalysis);
    when(mockAnalysis.getFrom()).thenReturn(mockAliasedDataSource);
    when(mockAliasedDataSource.getDataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(mockDataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(ksqlTopic.getKeyFormat()).thenReturn(keyFormat);
    when(keyFormat.isWindowed()).thenReturn(false);
    when(concurrencyLimiter.increment()).thenReturn(decrementer);
    doAnswer(a -> {
      final BiConsumer<Void, Throwable> biConsumer = a.getArgument(0);
      biConsumer.accept(null, null);
      return null;
    }).when(pullQueryResult).onCompletionOrException(any());
    when(pullQueryResult.getPlanType()).thenReturn(PullPhysicalPlanType.KEY_LOOKUP);
    when(pullQueryResult.getSourceType()).thenReturn(QuerySourceType.NON_WINDOWED);
    when(pullQueryResult.getTotalRowsProcessed()).thenReturn(ROWS_PROCESSED);
    when(pullQueryResult.getTotalRowsReturned()).thenReturn(ROWS_RETURNED);
    when(streamPullQueryMetadata.getTransientQueryMetadata()).thenReturn(transientQueryMetadata);
    when(transientQueryMetadata.getKafkaStreams()).thenReturn(kafkaStreams);
    when(transientQueryMetadata.getRowQueue()).thenReturn(transientQueryQueue);
    when(kafkaStreams.state()).thenReturn(State.NOT_RUNNING);
    when(scalablePushQueryMetadata.getRoutingNodeType()).thenReturn(RoutingNodeType.SOURCE_NODE);
    when(scalablePushQueryMetadata.getSourceType()).thenReturn(QuerySourceType.NON_WINDOWED);
    when(scalablePushQueryMetadata.getTotalRowsProcessed()).thenReturn(ROWS_PROCESSED);
    when(scalablePushQueryMetadata.getTotalRowsReturned()).thenReturn(ROWS_RETURNED);

    rateLimiter = new RateLimiter(
        1,
        "pull",
        new Metrics(),
        Collections.emptyMap()
    );
    final Query pullQueryQuery = mock(Query.class);
    when(pullQueryQuery.isPullQuery()).thenReturn(true);
    final Query pushQueryQuery = mock(Query.class);
    when(pushQueryQuery.isPullQuery()).thenReturn(false);
    when(pushQueryQuery.getFrom())
        .thenReturn(new AliasedRelation(new Table(SourceName.of("Foo")), SourceName.of("blah")));
    when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
        ImmutableSet.of(new QueryId("a")));
    when(pushQueryQuery.getRefinement())
        .thenReturn(Optional.of(RefinementInfo.of(OutputRefinement.CHANGES)));
    pullQuery = PreparedStatement.of(PULL_QUERY_STRING, pullQueryQuery);
    pushQuery = PreparedStatement.of(PUSH_QUERY_STRING, pushQueryQuery);
    queryExecutor = new QueryExecutor(ksqlEngine, ksqlRestConfig, ksqlConfig,
        Optional.of(pullQueryExecutorMetrics), Optional.of(scalablePushQueryMetrics), rateLimiter,
        concurrencyLimiter, pullBandRateLimiter, scalablePushBandRateLimiter,
        haRouting, pushRouting, Optional.of(localCommands));
  }

  @Test
  public void shouldThrowExceptionIfPullQueriesDisabledStream() {
    shouldThrowExceptionIfPullQueriesDisabled(DataSourceType.KSTREAM);
  }

  @Test
  public void shouldThrowExceptionIfPullQueriesDisabledTable() {
    shouldThrowExceptionIfPullQueriesDisabled(DataSourceType.KTABLE);
  }

  private void shouldThrowExceptionIfPullQueriesDisabled(final DataSourceType dataSourceType) {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)).thenReturn(false);
    when(mockDataSource.getDataSourceType()).thenReturn(dataSourceType);

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false)
    );

    // Then:
    final String errorMsg =
        "Pull queries are disabled. See https://cnfl.io/queries for more info.\n"
        + "Add EMIT CHANGES if you intended to issue a push query.\n"
        + "Please set ksql.pull.queries.enable=true to enable this feature.\n";
    assertThat(e.getMessage(), is(errorMsg));
  }

  @Test
  public void shouldRateLimit() {
    when(ksqlEngine.executeTablePullQuery(any(), any(), any(), any(), any(), any(), any(),
        anyBoolean(), any()))
        .thenReturn(pullQueryResult);
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    // When:
    queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
        pullQuery, Optional.empty(), metricsCallbackHolder, context, false);
    Exception e = assertThrows(KsqlException.class, () ->
        queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false));

    // Then:
    assertThat(e.getMessage(),
        is("Host is at rate limit for pull queries. Currently set to 1.0 qps."));
  }

  @Test
  public void shouldRateLimitStreamPullQueries() {
    when(ksqlEngine.createStreamPullQuery(any(), any(), any(), anyBoolean()))
        .thenReturn(streamPullQueryMetadata);

    // When:
    queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
        pullQuery, Optional.empty(), metricsCallbackHolder, context, false);
    Exception e = assertThrows(KsqlException.class, () ->
        queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false));

    // Then:
    assertThat(e.getMessage(),
        is("Host is at rate limit for pull queries. Currently set to 1.0 qps."));
  }

  @Test
  public void queryLoggerShouldReceiveStatementsWhenHandlePushQuery() {
    when(ksqlEngine.executeTransientQuery(any(), any(), anyBoolean()))
        .thenReturn(transientQueryMetadata);
    try (MockedStatic<QueryLogger> logger = Mockito.mockStatic(QueryLogger.class)) {
      queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
          pushQuery, Optional.empty(), metricsCallbackHolder, context, false);

      logger.verify(() -> QueryLogger.info("Transient query created",
          PUSH_QUERY_STRING), times(1));
    }
  }

  @Test
  public void queryLoggerShouldNotReceiveStatementsWhenHandlePullQuery() {
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(ksqlEngine.executeTablePullQuery(any(), any(), any(), any(), any(), any(), any(),
        anyBoolean(), any()))
        .thenReturn(pullQueryResult);
    try (MockedStatic<QueryLogger> logger = Mockito.mockStatic(QueryLogger.class)) {
      queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
          pullQuery, Optional.empty(), metricsCallbackHolder, context, false);

      logger.verify(() -> QueryLogger.info("Transient query created",
          PULL_QUERY_STRING), never());
    }
  }

  @Test
  public void shouldReachConcurrentLimitTablePullQuery() {
    // Given:
    when(concurrencyLimiter.increment()).thenThrow(new KsqlException("concurrencyLimiter Error!"));
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    // When:
    Exception e = assertThrows(KsqlException.class, () ->
        queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false));

    // Then:
    assertThat(e.getMessage(), containsString("concurrencyLimiter Error!"));
  }

  @Test
  public void shouldReachConcurrentLimitStreamPullQuery() {
    // Given:
    when(concurrencyLimiter.increment()).thenThrow(new KsqlException("concurrencyLimiter Error!"));
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);

    // When:
    Exception e = assertThrows(KsqlException.class, () ->
        queryExecutor.handleStatement(serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false));

    // Then:
    assertThat(e.getMessage(), containsString("concurrencyLimiter Error!"));
  }

  private QueryMetadataHolder expectPullQuerySuccess(
      final QuerySourceType sourceType,
      final PullPhysicalPlanType planType,
      final RoutingNodeType routingNodeType,
      final long rowsProcessed,
      final long rowsReturned
  ) {
    // When:
    final QueryMetadataHolder queryMetadataHolder = queryExecutor.handleStatement(
        serviceContext, ImmutableMap.of(), ImmutableMap.of(),
        pullQuery, Optional.empty(), metricsCallbackHolder, context, false);
    metricsCallbackHolder.reportMetrics(200, 1000L, 5000L, 20000L);

    // Then:
    verify(pullQueryExecutorMetrics, never()).recordLatencyForError(anyLong());
    verify(pullQueryExecutorMetrics).recordStatusCode(200);
    verify(pullQueryExecutorMetrics).recordRequestSize(1000L);
    verify(pullQueryExecutorMetrics).recordResponseSize(5000d,
        sourceType, planType, routingNodeType);
    verify(pullQueryExecutorMetrics).recordLatency(20000L,
        sourceType, planType, routingNodeType);
    verify(pullQueryExecutorMetrics).recordRowsProcessed(rowsProcessed,
        sourceType, planType, routingNodeType);
    verify(pullQueryExecutorMetrics).recordRowsReturned(rowsReturned,
        sourceType, planType, routingNodeType);
    verify(decrementer).decrementAtMostOnce();

    return queryMetadataHolder;
  }

  private void expectPullQueryError() {
    // When:
    Exception e = assertThrows(RuntimeException.class,
        () -> queryExecutor.handleStatement(
            serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pullQuery, Optional.empty(), metricsCallbackHolder, context, false));
    metricsCallbackHolder.reportMetrics(500, 1000L, 5000L, 20000L);

    // Then:
    assertThat(e.getMessage(), is("Error executing!"));
    verify(pullQueryExecutorMetrics).recordStatusCode(500);
    verify(pullQueryExecutorMetrics).recordRequestSize(1000L);
    verify(pullQueryExecutorMetrics).recordResponseSizeForError(5000L);
    verify(pullQueryExecutorMetrics).recordLatencyForError(20000L);
    verify(pullQueryExecutorMetrics).recordZeroRowsProcessedForError();
    verify(pullQueryExecutorMetrics).recordZeroRowsReturnedForError();
    verify(decrementer).decrementAtMostOnce();
  }

  @Test
  public void shouldRunTablePullQuery_success() {
    // Given:
    when(ksqlEngine.executeTablePullQuery(any(), any(), any(), any(), any(), any(), any(),
        anyBoolean(), any()))
        .thenReturn(pullQueryResult);
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    // Then:
    final QueryMetadataHolder queryMetadataHolder = expectPullQuerySuccess(
        QuerySourceType.NON_WINDOWED, PullPhysicalPlanType.KEY_LOOKUP, RoutingNodeType.SOURCE_NODE,
        ROWS_PROCESSED, ROWS_RETURNED);
    assertThat(queryMetadataHolder.getPullQueryResult().isPresent(), is(true));
  }

  @Test
  public void shouldRunTablePullQuery_errorExecuting() {
    // Given:
    when(ksqlEngine.executeTablePullQuery(any(), any(), any(), any(), any(), any(), any(),
        anyBoolean(), any()))
        .thenThrow(new RuntimeException("Error executing!"));
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    // Then:
    expectPullQueryError();
  }

  @Test
  public void shouldRunStreamPullQuery_success() {
    // Given:
    when(ksqlEngine.createStreamPullQuery(any(), any(), any(), anyBoolean()))
        .thenReturn(streamPullQueryMetadata);
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(transientQueryQueue.getTotalRowsQueued()).thenReturn(300L);

    // Then:
    final QueryMetadataHolder queryMetadataHolder = expectPullQuerySuccess(
        QuerySourceType.NON_WINDOWED_STREAM, PullPhysicalPlanType.UNKNOWN,
        RoutingNodeType.SOURCE_NODE, 300L, 300L);
    assertThat(queryMetadataHolder.getStreamPullQueryMetadata().isPresent(), is(true));
    verify(localCommands).write(transientQueryMetadata);
  }

  @Test
  public void shouldRunStreamPullQuery_errorExecuting() {
    // Given:
    when(ksqlEngine.createStreamPullQuery(any(), any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("Error executing!"));
    when(mockDataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);

    // Then:
    expectPullQueryError();
  }

  @Test
  public void shouldRunPushQuery_success() {
    // Given:
    when(ksqlEngine.executeTransientQuery(any(), any(), anyBoolean()))
        .thenReturn(transientQueryMetadata);

    // When:
    final QueryMetadataHolder queryMetadataHolder = queryExecutor.handleStatement(
        serviceContext, ImmutableMap.of(), ImmutableMap.of(),
        pushQuery, Optional.empty(), metricsCallbackHolder, context, false);
    // Should be no metrics reported for push queries
    metricsCallbackHolder.reportMetrics(200, 1000L, 5000L, 20000L);

    // Then:
    verifyNoMoreInteractions(pullQueryExecutorMetrics);
    assertThat(queryMetadataHolder.getTransientQueryMetadata().isPresent(), is(true));
    verify(localCommands).write(transientQueryMetadata);
  }

  @Test
  public void shouldRunPushQuery_error() {
    // Given:
    when(ksqlEngine.executeTransientQuery(any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("Error executing!"));

    // When:
    Exception e = assertThrows(RuntimeException.class,
        () -> queryExecutor.handleStatement(
            serviceContext, ImmutableMap.of(), ImmutableMap.of(),
            pushQuery, Optional.empty(), metricsCallbackHolder, context, false));
    // Should be no metrics reported for push queries
    metricsCallbackHolder.reportMetrics(200, 1000L, 5000L, 20000L);

    // Then:
    assertThat(e.getMessage(), is("Error executing!"));
    verifyNoMoreInteractions(pullQueryExecutorMetrics);
  }

  @Test
  public void shouldRunScalablePushQuery_success() {
    // Given:
    when(ksqlEngine.executeScalablePushQuery(any(), any(), any(), any(), any(), any(), any(),
        any()))
        .thenReturn(scalablePushQueryMetadata);

    // When:
    final QueryMetadataHolder queryMetadataHolder = queryExecutor.handleStatement(
        serviceContext,
        ImmutableMap.of(
            KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"),
        ImmutableMap.of(),
        pushQuery, Optional.empty(), metricsCallbackHolder, context, false);
    // Should be no metrics reported for push queries
    metricsCallbackHolder.reportMetrics(200, 1000L, 5000L, 20000L);

    // Then:
    verifyNoMoreInteractions(pullQueryExecutorMetrics);
    assertThat(queryMetadataHolder.getScalablePushQueryMetadata().isPresent(), is(true));
    verify(scalablePushQueryMetrics).recordStatusCode(200);
    verify(scalablePushQueryMetrics).recordRequestSize(1000L);
    verify(scalablePushQueryMetrics).recordResponseSize(5000d,
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    verify(scalablePushQueryMetrics).recordConnectionDuration(20000L,
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    verify(scalablePushQueryMetrics).recordRowsProcessed(ROWS_PROCESSED,
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
    verify(scalablePushQueryMetrics).recordRowsReturned(ROWS_RETURNED,
        QuerySourceType.NON_WINDOWED, RoutingNodeType.SOURCE_NODE);
  }

  @Test
  public void shouldRunScalablePushQuery_error() {
    // Given:
    when(ksqlEngine.executeScalablePushQuery(any(), any(), any(), any(), any(), any(), any(),
        any()))
        .thenThrow(new RuntimeException("Error executing!"));

    // When:
    Exception e = assertThrows(RuntimeException.class,
        () -> queryExecutor.handleStatement(
            serviceContext, ImmutableMap.of(
                KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"),
            ImmutableMap.of(),
            pushQuery, Optional.empty(), metricsCallbackHolder, context, false));
    // Should be no metrics reported for push queries
    metricsCallbackHolder.reportMetrics(500, 1000L, 5000L, 20000L);

    // Then:
    assertThat(e.getMessage(), is("Error executing!"));
    verifyNoMoreInteractions(pullQueryExecutorMetrics);
    verify(scalablePushQueryMetrics).recordStatusCode(500);
    verify(scalablePushQueryMetrics).recordRequestSize(1000L);
    verify(scalablePushQueryMetrics).recordResponseSizeForError(5000L);
    verify(scalablePushQueryMetrics).recordConnectionDurationForError(20000L);
    verify(scalablePushQueryMetrics).recordZeroRowsProcessedForError();
    verify(scalablePushQueryMetrics).recordZeroRowsReturnedForError();
  }
}