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

package io.confluent.ksql.engine;

import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ExecutableDdlStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlEngine implements KsqlExecutionContext, Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlEngine.class);

  private final KsqlEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final EngineContext primaryContext;
  private final QueryCleanupService cleanupService;
  private final OrphanedTransientQueryCleaner orphanedTransientQueryCleaner;
  private final KsqlConfig ksqlConfig;
  private final Admin persistentAdminClient;

  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final ServiceInfo serviceInfo,
      final QueryIdGenerator queryIdGenerator,
      final KsqlConfig ksqlConfig,
      final List<QueryEventListener> queryEventListeners
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceInfo.serviceId(),
        new MetaStoreImpl(functionRegistry),
        (engine) -> new KsqlEngineMetrics(
            serviceInfo.metricsPrefix(),
            engine,
            serviceInfo.customMetricsTags(),
            serviceInfo.metricsExtension()
        ),
        queryIdGenerator,
        ksqlConfig,
        queryEventListeners
    );
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public KsqlEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KsqlEngine, KsqlEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator,
      final KsqlConfig ksqlConfig,
      final List<QueryEventListener> queryEventListeners
  ) {
    this.cleanupService = new QueryCleanupService();
    this.orphanedTransientQueryCleaner =
        new OrphanedTransientQueryCleaner(this.cleanupService, ksqlConfig);
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        cleanupService,
        ksqlConfig,
        ImmutableList.<QueryEventListener>builder()
            .addAll(queryEventListeners)
            .add(engineMetrics.getQueryEventListener())
            .add(new CleanupListener(cleanupService, serviceContext, ksqlConfig))
            .build()
    );
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.aggregateMetricsCollector.scheduleAtFixedRate(
        () -> {
          try {
            this.engineMetrics.updateMetrics();
          } catch (final Exception e) {
            log.info("Error updating engine metrics", e);
          }
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS
    );
    this.ksqlConfig = ksqlConfig;
    this.persistentAdminClient = Admin.create(ksqlConfig.getKsqlAdminClientConfigProps());

    cleanupService.startAsync();
  }

  public int numberOfLiveQueries() {
    return primaryContext.getQueryRegistry().getAllLiveQueries().size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getQueryRegistry().getPersistentQuery(queryId);
  }

  @Override
  public Optional<QueryMetadata> getQuery(final QueryId queryId) {
    return primaryContext.getQueryRegistry().getQuery(queryId);
  }

  @Override
  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.getQueryRegistry().getPersistentQueries().values());
  }

  @Override
  public Set<QueryId> getQueriesWithSink(final SourceName sourceName) {
    return primaryContext.getQueryRegistry().getQueriesWithSink(sourceName);
  }

  @Override
  public List<QueryMetadata> getAllLiveQueries() {
    return primaryContext.getQueryRegistry().getAllLiveQueries();
  }

  public boolean hasActiveQueries() {
    return !primaryContext.getQueryRegistry().getPersistentQueries().isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.getMetaStore();
  }

  @Override
  public ServiceContext getServiceContext() {
    return primaryContext.getServiceContext();
  }

  @Override
  public ProcessingLogContext getProcessingLogContext() {
    return primaryContext.getProcessingLogContext();
  }

  public String getServiceId() {
    return serviceId;
  }

  @VisibleForTesting
  QueryCleanupService getCleanupService() {
    return cleanupService;
  }

  @Override
  public KsqlExecutionContext createSandbox(final ServiceContext serviceContext) {
    return new SandboxedExecutionContext(primaryContext, serviceContext);
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return primaryContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(
      final ParsedStatement stmt,
      final Map<String, String> variablesMap
  ) {
    return primaryContext.prepare(stmt, variablesMap);
  }

  @Override
  public KsqlPlan plan(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return EngineExecutor
        .create(primaryContext, serviceContext, statement.getSessionConfig())
        .plan(statement);
  }

  @Override
  public ExecuteResult execute(final ServiceContext serviceContext, final ConfiguredKsqlPlan plan) {
    try {
      final ExecuteResult result = EngineExecutor
          .create(primaryContext, serviceContext, plan.getConfig())
          .execute(plan.getPlan());
      return result;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      // add the statement text to the KsqlException
      throw new KsqlStatementException(
          e.getMessage(),
          plan.getPlan().getStatementText(),
          e.getCause()
      );
    }
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    return execute(
        serviceContext,
        ConfiguredKsqlPlan.of(
            plan(serviceContext, statement),
            statement.getSessionConfig()
        )
    );
  }

  @Override
  public TransientQueryMetadata executeTransientQuery(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final boolean excludeTombstones
  ) {
    try {
      final TransientQueryMetadata query = EngineExecutor
          .create(primaryContext, serviceContext, statement.getSessionConfig())
          .executeTransientQuery(statement, excludeTombstones);
      return query;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      // add the statement text to the KsqlException
      throw new KsqlStatementException(e.getMessage(), statement.getStatementText(), e.getCause());
    }
  }

  /**
   * Unlike the other queries, stream pull queries are split into create and wait because the three
   * API endpoints all need to do different stuff before, in the middle of, and after these two
   * phases. One of them actually needs to wait on the pull query in a callback after starting the
   * query, so splitting it into two method calls was the most practical choice.
   */
  public StreamPullQueryMetadata createStreamPullQuery(
      final ServiceContext serviceContext,
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statementOrig,
      final boolean excludeTombstones
  ) {

    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED)) {
      throw new KsqlStatementException(
          "Pull queries on streams are disabled. To create a push query on the stream,"
              + " add EMIT CHANGES to the end. To enable pull queries on streams, set"
              + " the " + KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED + " config to 'true'.",
          statementOrig.getStatementText()
      );
    }

    // Stream pull query overrides: start from earliest, use one  thread, and use a tight commit
    // interval for responsiveness.
    // Starting from earliest is semantically necessary.
    // Using a single thread keeps these queries as lightweight as possible, since we are
    // not counting them against the transient query limit.
    // Setting the commit interval to 100ms to make the query results snappier.
    // Since this is a "sit and wait", not a "fire and forget", query,
    // we want to make sure people are going to see their results come back asap.
    final ConfiguredStatement<Query> statement = statementOrig.withConfigOverrides(
        ImmutableMap.<String, Object>builder()
            .putAll(statementOrig.getSessionConfig().getOverrides())
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
            .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100)
            .build()
    );
    final TransientQueryMetadata transientQueryMetadata = EngineExecutor
        .create(primaryContext, serviceContext, statement.getSessionConfig())
        .executeTransientQuery(statement, excludeTombstones);

    final ImmutableMap<TopicPartition, Long> endOffsets =
        getQueryInputEndOffsets(analysis, statement, serviceContext.getAdminClient());

    QueryLogger.info(
        "Streaming stream pull query results '{}'",
        statement.getStatementText()
    );

    return new StreamPullQueryMetadata(transientQueryMetadata, endOffsets);
  }

  public boolean passedEndOffsets(final StreamPullQueryMetadata streamPullQueryMetadata) {

    try {
      final ListConsumerGroupOffsetsResult result =
          persistentAdminClient.listConsumerGroupOffsets(
              streamPullQueryMetadata.getTransientQueryMetadata().getQueryApplicationId()
          );

      final Map<TopicPartition, OffsetAndMetadata> metadataMap =
          result.partitionsToOffsetAndMetadata().get();

      final Map<TopicPartition, Long> endOffsets = streamPullQueryMetadata.getEndOffsets();

      for (final Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        final OffsetAndMetadata offsetAndMetadata = metadataMap.get(entry.getKey());
        if (offsetAndMetadata == null || offsetAndMetadata.offset() < entry.getValue()) {
          log.debug("SPQ waiting on " + entry + " at " + offsetAndMetadata);
          return false;
        }
      }
      return true;
    } catch (final ExecutionException | InterruptedException e) {
      throw new KsqlException(e);
    }
  }

  private ImmutableMap<TopicPartition, Long> getQueryInputEndOffsets(
      final ImmutableAnalysis analysis,
      final ConfiguredStatement<Query> statement,
      final Admin admin) {

    final String sourceTopicName = analysis.getFrom().getDataSource().getKafkaTopicName();
    final TopicDescription topicDescription = getTopicDescription(
        admin,
        sourceTopicName
    );

    final Object processingGuarantee = statement
        .getSessionConfig()
        .getConfig(true)
        .getKsqlStreamConfigProps()
        .get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

    final IsolationLevel isolationLevel =
        StreamsConfig.AT_LEAST_ONCE.equals(processingGuarantee)
            ? IsolationLevel.READ_UNCOMMITTED
            : IsolationLevel.READ_COMMITTED;

    return getEndOffsets(
        admin,
        topicDescription,
        isolationLevel
    );
  }

  private TopicDescription getTopicDescription(final Admin admin, final String sourceTopicName) {
    final KafkaFuture<TopicDescription> topicDescriptionKafkaFuture = admin
        .describeTopics(Collections.singletonList(sourceTopicName))
        .values()
        .get(sourceTopicName);

    try {
      return topicDescriptionKafkaFuture.get(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      log.error("Admin#describeTopics(" + sourceTopicName + ") interrupted", e);
      throw new KsqlServerException("Interrupted");
    } catch (final ExecutionException e) {
      log.error("Error executing Admin#describeTopics(" + sourceTopicName + ")", e);
      throw new KsqlServerException("Internal Server Error");
    } catch (final TimeoutException e) {
      log.error("Admin#describeTopics(" + sourceTopicName + ") timed out", e);
      throw new KsqlServerException("Backend timed out");
    }
  }

  private ImmutableMap<TopicPartition, Long> getEndOffsets(
      final Admin admin,
      final TopicDescription topicDescription,
      final IsolationLevel isolationLevel) {
    final Map<TopicPartition, OffsetSpec> topicPartitions =
        topicDescription
            .partitions()
            .stream()
            .map(td -> new TopicPartition(topicDescription.name(), td.partition()))
            .collect(toMap(identity(), tp -> OffsetSpec.latest()));

    final ListOffsetsResult listOffsetsResult = admin.listOffsets(
        topicPartitions,
        new ListOffsetsOptions(
            isolationLevel
        )
    );

    try {
      final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> partitionResultMap =
          listOffsetsResult.all().get(10, TimeUnit.SECONDS);
      final Map<TopicPartition, Long> result = partitionResultMap
          .entrySet()
          .stream()
          // special case where we expect no work at all on the partition, so we don't even
          // need to check the committed offset (if we did, we'd potentially wait forever,
          // since Streams won't commit anything for an empty topic).
          .filter(e -> e.getValue().offset() > 0L)
          .collect(toMap(Entry::getKey, e -> e.getValue().offset()));
      return ImmutableMap.copyOf(result);
    } catch (final InterruptedException e) {
      log.error("Admin#listOffsets(" + topicDescription.name() + ") interrupted", e);
      throw new KsqlServerException("Interrupted");
    } catch (final ExecutionException e) {
      log.error("Error executing Admin#listOffsets(" + topicDescription.name() + ")", e);
      throw new KsqlServerException("Internal Server Error");
    } catch (final TimeoutException e) {
      log.error("Admin#listOffsets(" + topicDescription.name() + ") timed out", e);
      throw new KsqlServerException("Backend timed out");
    }
  }

  @Override
  public ScalablePushQueryMetadata executeScalablePushQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final PushRouting pushRouting,
      final PushRoutingOptions pushRoutingOptions,
      final QueryPlannerOptions queryPlannerOptions,
      final Context context
  ) {
    final ScalablePushQueryMetadata query = EngineExecutor
        .create(primaryContext, serviceContext, statement.getSessionConfig())
        .executeScalablePushQuery(
            analysis,
            statement,
            pushRouting,
            pushRoutingOptions,
            queryPlannerOptions,
            context);
    return query;
  }

  @Override
  public PullQueryResult executeTablePullQuery(
      final ImmutableAnalysis analysis,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> statement,
      final HARouting routing,
      final RoutingOptions routingOptions,
      final QueryPlannerOptions plannerOptions,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final boolean startImmediately
  ) {
    return EngineExecutor
        .create(
            primaryContext,
            serviceContext,
            statement.getSessionConfig()
        )
        .executeTablePullQuery(
            analysis,
            statement,
            routing,
            routingOptions,
            plannerOptions,
            pullQueryMetrics,
            startImmediately
        );
  }

  /**
   * @param closeQueries whether or not to clean up the local state for any running queries
   */
  public void close(final boolean closeQueries) {
    primaryContext.getQueryRegistry().close(closeQueries);

    try {
      cleanupService.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      log.warn("Timed out while closing cleanup service. "
              + "External resources for the following applications may be orphaned: {}",
          cleanupService.pendingApplicationIds()
      );
    }

    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
    persistentAdminClient.close();
  }

  @Override
  public void close() {
    close(false);
  }

  public void cleanupOrphanedInternalTopics(
      final ServiceContext serviceContext,
      final Set<String> queryApplicationIds
  ) {
    orphanedTransientQueryCleaner
        .cleanupOrphanedInternalTopics(serviceContext, queryApplicationIds);
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final Statement statement) {
    return statement instanceof ExecutableDdlStatement
        || statement instanceof QueryContainer
        || statement instanceof Query;
  }

  /**
   * For analyzing queries that you know won't have an output topic, such as pull queries.
   */
  public ImmutableAnalysis analyzeQueryWithNoOutputTopic(
      final Query query,
      final String queryText) {

    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(getMetaStore(), "");
    final Analysis analysis;
    try {
      analysis = queryAnalyzer.analyze(query, Optional.empty());
    } catch (final KsqlException e) {
      throw new KsqlStatementException(e.getMessage(), queryText, e);
    }
    return new RewrittenAnalysis(
        analysis,
        new QueryExecutionUtil.ColumnReferenceRewriter()::process
    );
  }

  private static final class CleanupListener implements QueryEventListener {
    final QueryCleanupService cleanupService;
    final ServiceContext serviceContext;
    final KsqlConfig ksqlConfig;

    private CleanupListener(
        final QueryCleanupService cleanupService,
        final ServiceContext serviceContext,
        final KsqlConfig ksqlConfig) {
      this.cleanupService = cleanupService;
      this.serviceContext = serviceContext;
      this.ksqlConfig = ksqlConfig;
    }

    @Override
    public void onClose(
        final QueryMetadata query
    ) {
      final String applicationId = query.getQueryApplicationId();
      if (query.hasEverBeenStarted()) {
        cleanupService.addCleanupTask(
            new QueryCleanupService.QueryCleanupTask(
                serviceContext,
                applicationId,
                query instanceof TransientQueryMetadata,
                ksqlConfig.getKsqlStreamConfigProps()
                    .getOrDefault(
                        StreamsConfig.STATE_DIR_CONFIG,
                        StreamsConfig.configDef()
                          .defaultValues()
                          .get(StreamsConfig.STATE_DIR_CONFIG))
                    .toString()
            ));
      }

      StreamsErrorCollector.notifyApplicationClose(applicationId);
    }
  }
}
