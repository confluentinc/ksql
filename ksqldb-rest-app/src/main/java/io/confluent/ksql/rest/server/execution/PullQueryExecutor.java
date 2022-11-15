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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.rest.entity.TableRows;
import io.confluent.ksql.rest.entity.TableRowsFactory;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("UnstableApiUsage")
public final class PullQueryExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(PullQueryExecutor.class);

  private static final Set<Type> VALID_WINDOW_BOUNDS_TYPES = ImmutableSet.of(
      Type.EQUAL,
      Type.GREATER_THAN,
      Type.GREATER_THAN_OR_EQUAL,
      Type.LESS_THAN,
      Type.LESS_THAN_OR_EQUAL
  );

  private static final String VALID_WINDOW_BOUNDS_COLUMNS =
      GrammaticalJoiner.and().join(SystemColumns.windowBoundsColumnNames());

  private static final String VALID_WINDOW_BOUNDS_TYPES_STRING =
      GrammaticalJoiner.and().join(VALID_WINDOW_BOUNDS_TYPES);

  private final KsqlExecutionContext executionContext;
  private final RoutingFilterFactory routingFilterFactory;
  private final RateLimiter rateLimiter;
  private final ExecutorService executorService;

  public PullQueryExecutor(
      final KsqlExecutionContext executionContext,
      final RoutingFilterFactory routingFilterFactory,
      final KsqlConfig ksqlConfig
  ) {
    this(
        executionContext,
        routingFilterFactory,
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG),
        Executors.newFixedThreadPool(
            ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG)
        )
    );
  }

  @VisibleForTesting
  PullQueryExecutor(
      final KsqlExecutionContext executionContext,
      final RoutingFilterFactory routingFilterFactory,
      final int maxQps, final ExecutorService executorService
  ) {
    this.executionContext = requireNonNull(executionContext, "executionContext");
    this.routingFilterFactory = requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = RateLimiter.create(maxQps);
    this.executorService = requireNonNull(executorService, "executorService");
  }

  @SuppressWarnings("unused") // Needs to match validator API.
  public static void validate(
      final ConfiguredStatement<Query> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    throw new KsqlRestException(Errors.queryEndpoint(statement.getMaskedStatementText()));
  }

  public PullQueryResult execute(
      final ConfiguredStatement<Query> statement,
      final Map<String, Object> requestProperties,
      final ServiceContext serviceContext,
      final Optional<Boolean> isInternalRequest,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics
  ) {
    if (!statement.getStatement().isPullQuery()) {
      throw new IllegalArgumentException("Executor can only handle pull queries");
    }

    final SessionConfig sessionConfig = statement.getSessionConfig();

    if (!sessionConfig.getConfig(false)
        .getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)) {
      throw new KsqlStatementException(
          "Pull queries are disabled."
              + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
              + System.lineSeparator()
              + "Please set " + KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG + "=true to enable "
              + "this feature.",
          statement.getMaskedStatementText());
    }

    try {
      // Not using session.getConfig(true) due to performance issues,
      // see: https://github.com/confluentinc/ksql/issues/6407
      final RoutingOptions routingOptions = new ConfigRoutingOptions(
          sessionConfig.getConfig(false),
          statement.getSessionConfig().getOverrides(),
          requestProperties
      );

      // If internal listeners are in use, we require the request to come from that listener to
      // treat it as having been forwarded.
      final boolean isAlreadyForwarded = routingOptions.skipForwardRequest()
          // Trust the forward request option if isInternalRequest isn't available.
          && isInternalRequest.orElse(true);

      // Only check the rate limit at the forwarding host
      if (!isAlreadyForwarded) {
        checkRateLimit();
      }

      final ImmutableAnalysis analysis = new RewrittenAnalysis(
          analyze(statement, executionContext),
          new ColumnReferenceRewriter()::process
      );

      final PersistentQueryMetadata query = findMaterializingQuery(executionContext, analysis);

      final WhereInfo whereInfo = extractWhereInfo(analysis, query);

      final QueryId queryId = uniqueQueryId();

      final QueryContext.Stacker contextStacker = new Stacker();

      final Materialization mat = query
          .getMaterialization(queryId, contextStacker)
          .orElseThrow(() -> notMaterializedException(getSourceName(analysis)));

      final List<Struct> keys = whereInfo.keysBound.stream()
          .map(keyBound -> asKeyStruct(keyBound, query.getPhysicalSchema()))
          .collect(ImmutableList.toImmutableList());

      final List<KsqlPartitionLocation> locations = mat.locator().locate(
          keys,
          routingOptions,
          routingFilterFactory
      );

      final Function<List<KsqlPartitionLocation>, PullQueryContext> contextFactory
          = (locationsForHost) ->
          new PullQueryContext(
              locationsForHost,
              mat,
              analysis,
              whereInfo,
              queryId,
              contextStacker,
              pullQueryMetrics);

      return handlePullQuery(
          statement,
          executionContext,
          serviceContext,
          routingOptions,
          contextFactory,
          queryId,
          locations,
          executorService,
          PullQueryExecutor::routeQuery);

    } catch (final Exception e) {
      pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1));
      throw new KsqlStatementException(
          e.getMessage() == null ? "Server Error" : e.getMessage(),
          statement.getMaskedStatementText(),
          e
      );
    }
  }

  public void close(final Duration timeout) {
    try {
      executorService.shutdown();
      executorService.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void validateSchemas(final List<LogicalSchema> schemas) {
    final LogicalSchema schema = Iterables.getLast(schemas);
    for (LogicalSchema s : schemas) {
      if (!schema.equals(s)) {
        throw new KsqlException("Schemas from different hosts should be identical");
      }
    }
  }

  @VisibleForTesting
  void checkRateLimit() {
    if (!rateLimiter.tryAcquire()) {
      throw new KsqlException("Host is at rate limit for pull queries. Currently set to "
          + rateLimiter.getRate() + " qps.");
    }
  }

  @VisibleForTesting
  interface RouteQuery {
    TableRows routeQuery(
        KsqlNode node,
        ConfiguredStatement<Query> statement,
        KsqlExecutionContext executionContext,
        ServiceContext serviceContext,
        PullQueryContext pullQueryContext
    );
  }

  @VisibleForTesting
  static PullQueryResult handlePullQuery(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final RoutingOptions routingOptions,
      final Function<List<KsqlPartitionLocation>, PullQueryContext> contextFactory,
      final QueryId queryId,
      final List<KsqlPartitionLocation> locations,
      final ExecutorService executorService,
      final RouteQuery routeQuery
  ) throws InterruptedException {
    final boolean anyPartitionsEmpty = locations.stream()
        .anyMatch(location -> location.getNodes().isEmpty());
    if (anyPartitionsEmpty) {
      LOG.debug("Unable to execute pull query: {}. All nodes are dead or exceed max allowed lag.",
          statement.getMaskedStatementText());
      throw new MaterializationException(String.format(
          "Unable to execute pull query %s. All nodes are dead or exceed max allowed lag.",
          statement.getMaskedStatementText()));
    }

    // The source nodes associated with each of the rows
    final List<KsqlNode> sourceNodes = new ArrayList<>();
    // Each of the table rows returned, aggregated across nodes
    final List<List<?>> tableRows = new ArrayList<>();
    // Each of the schemas returned, aggregated across nodes
    final List<LogicalSchema> schemas = new ArrayList<>();
    List<KsqlPartitionLocation> remainingLocations = ImmutableList.copyOf(locations);
    // For each round, each set of partition location objects is grouped by host, and all
    // keys associated with that host are batched together. For any requests that fail,
    // the partition location objects will be added to remainingLocations, and the next round
    // will attempt to fetch them from the next node in their prioritized list.
    // For example, locations might be:
    // [ Partition 0 <Host 1, Host 2>,
    //   Partition 1 <Host 2, Host 1>,
    //   Partition 2 <Host 1, Host 2> ]
    // In Round 0, fetch from Host 1: [Partition 0, Partition 2], from Host 2: [Partition 1]
    // If everything succeeds, we're done.  If Host 1 failed, then we'd have a Round 1:
    // In Round 1, fetch from Host 2: [Partition 0, Partition 2].
    for (int round = 0; ; round++) {
      // Group all partition location objects by their nth round node
      final Map<KsqlNode, List<KsqlPartitionLocation>> groupedByHost
          = groupByHost(statement, remainingLocations, round);

      // Make requests to each host, specifying the partitions we're interested in from
      // this host.
      final Map<KsqlNode, Future<PullQueryResult>> futures = new LinkedHashMap<>();
      for (Map.Entry<KsqlNode, List<KsqlPartitionLocation>> entry : groupedByHost.entrySet()) {
        final KsqlNode node = entry.getKey();
        final PullQueryContext pullQueryContext = contextFactory.apply(entry.getValue());

        futures.put(node, executorService.submit(() ->  {
          final TableRows rows = routeQuery.routeQuery(
              node, statement, executionContext, serviceContext, pullQueryContext);
          final Optional<List<KsqlNode>> debugNodes = Optional.ofNullable(
              routingOptions.isDebugRequest()
                  ? Collections.nCopies(rows.getRows().size(), node) : null);
          return new PullQueryResult(rows, debugNodes);
        }));
      }

      // Go through all of the results of the requests, either aggregating rows or adding
      // the locations to the nextRoundRemaining list.
      final ImmutableList.Builder<KsqlPartitionLocation> nextRoundRemaining
          = ImmutableList.builder();
      for (Map.Entry<KsqlNode, Future<PullQueryResult>>  entry : futures.entrySet()) {
        final Future<PullQueryResult> future = entry.getValue();
        final KsqlNode node = entry.getKey();
        try {
          final PullQueryResult result = future.get();
          result.getSourceNodes().ifPresent(sourceNodes::addAll);
          schemas.add(result.getTableRows().getSchema());
          tableRows.addAll(result.getTableRows().getRows());
        } catch (ExecutionException e) {
          LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
              statement.getMaskedStatementText(), node, System.currentTimeMillis(), e.getCause());
          nextRoundRemaining.addAll(groupedByHost.get(node));
        }
      }
      remainingLocations = nextRoundRemaining.build();

      // If there are no partition locations remaining, then we're done.
      if (remainingLocations.size() == 0) {
        validateSchemas(schemas);
        return new PullQueryResult(
            new TableRows(statement.getMaskedStatementText(), queryId, Iterables.getLast(schemas),
                tableRows),
            sourceNodes.isEmpty() ? Optional.empty() : Optional.of(sourceNodes));
      }
    }
  }

  /**
   * Groups all of the partition locations by the round-th entry in their prioritized list
   * of host nodes.
   * @param statement the statement from which this request came
   * @param locations the list of partition locations to parse
   * @param round which round this is
   * @return A map of node to list of partition locations
   */
  private static Map<KsqlNode, List<KsqlPartitionLocation>> groupByHost(
      final ConfiguredStatement<Query> statement,
      final List<KsqlPartitionLocation> locations,
      final int round) {
    final Map<KsqlNode, List<KsqlPartitionLocation>> groupedByHost = new LinkedHashMap<>();
    for (KsqlPartitionLocation location : locations) {
      // If one of the partitions required is out of nodes, then we cannot continue.
      if (round >= location.getNodes().size()) {
        throw new MaterializationException(String.format(
            "Unable to execute pull query: %s. Exhausted standby hosts to try.",
            statement.getMaskedStatementText()));
      }
      final KsqlNode nextHost = location.getNodes().get(round);
      groupedByHost.computeIfAbsent(nextHost, h -> new ArrayList<>()).add(location);
    }
    return groupedByHost;
  }

  private static TableRows routeQuery(
      final KsqlNode node,
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final PullQueryContext pullQueryContext
  ) {
    if (node.isLocal()) {
      LOG.debug("Query {} executed locally at host {} at timestamp {}.",
               statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
      pullQueryContext.pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
      return queryRowsLocally(
          statement,
          executionContext,
          pullQueryContext);
    } else {
      LOG.debug("Query {} routed to host {} at timestamp {}.",
                statement.getMaskedStatementText(), node.location(), System.currentTimeMillis());
      pullQueryContext.pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
      return forwardTo(node, statement, serviceContext, pullQueryContext);
    }
  }

  private static TableRows queryRowsLocally(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final PullQueryContext pullQueryContext
  ) {
    final Result result;
    if (pullQueryContext.whereInfo.windowBounds.isPresent()) {
      final WindowBounds windowBounds = pullQueryContext.whereInfo.windowBounds.get();

      final ImmutableList.Builder<TableRow> allRows = ImmutableList.builder();
      for (KsqlPartitionLocation location : pullQueryContext.locations) {
        if (!location.getKeys().isPresent()) {
          throw new IllegalStateException("Window queries should be done with keys");
        }
        for (Struct key : location.getKeys().get()) {
          final List<? extends TableRow> rows = pullQueryContext.mat.windowed()
              .get(key, location.getPartition(), windowBounds.start,
                  windowBounds.end);
          allRows.addAll(rows);
        }
      }
      result = new Result(pullQueryContext.mat.schema(), allRows.build());
    } else {
      final ImmutableList.Builder<TableRow> allRows = ImmutableList.builder();
      for (KsqlPartitionLocation location : pullQueryContext.locations) {
        if (!location.getKeys().isPresent()) {
          throw new IllegalStateException("Window queries should be done with keys");
        }
        for (Struct key : location.getKeys().get()) {
          final List<? extends TableRow> rows = pullQueryContext.mat.nonWindowed()
              .get(key, location.getPartition())
              .map(ImmutableList::of)
              .orElse(ImmutableList.of());
          allRows.addAll(rows);
        }
      }
      result = new Result(pullQueryContext.mat.schema(), allRows.build());
    }

    final LogicalSchema outputSchema;
    final List<List<?>> rows;
    if (isSelectStar(statement.getStatement().getSelect())) {
      outputSchema = TableRowsFactory.buildSchema(
          result.schema, pullQueryContext.mat.windowType().isPresent());
      rows = TableRowsFactory.createRows(result.rows);
    } else {
      final List<SelectExpression> projection = pullQueryContext.analysis.getSelectItems().stream()
          .map(SingleColumn.class::cast)
          .map(si -> SelectExpression
              .of(si.getAlias().orElseThrow(IllegalStateException::new), si.getExpression()))
          .collect(Collectors.toList());

      outputSchema = selectOutputSchema(
          result, executionContext, projection, pullQueryContext.mat.windowType());

      rows = handleSelects(
          result,
          statement,
          executionContext,
          pullQueryContext.analysis,
          outputSchema,
          projection,
          pullQueryContext.mat.windowType(),
          pullQueryContext.queryId,
          pullQueryContext.contextStacker
      );
    }
    return new TableRows(
        statement.getMaskedStatementText(),
        pullQueryContext.queryId,
        outputSchema,
        rows
    );
  }

  private static TableRows forwardTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PullQueryContext pullQueryContext
  ) {
    // Specify the partitions we specifically want to read.  This will prevent reading unintended
    // standby data when we are reading active for example.
    final String partitions = pullQueryContext.locations.stream()
        .map(location -> Integer.toString(location.getPartition()))
        .collect(Collectors.joining(","));
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true,
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS, partitions);
    final RestResponse<List<StreamedRow>> response = serviceContext
        .getKsqlClient()
        .makeQueryRequest(
            owner.location(),
            statement.getUnMaskedStatementText(),
            statement.getSessionConfig().getOverrides(),
            requestProperties
        );

    if (response.isErroneous()) {
      throw new KsqlServerException("Forwarding attempt failed: " + response.getErrorMessage());
    }

    final List<StreamedRow> streamedRows = response.getResponse();
    if (streamedRows.isEmpty()) {
      throw new KsqlServerException("Invalid empty response from forwarding call");
    }

    final Header header = streamedRows.get(0).getHeader()
        .orElseThrow(() -> new KsqlServerException("Expected header in first row"));

    final ImmutableList.Builder<List<?>> rows = ImmutableList.builder();

    for (final StreamedRow row : streamedRows.subList(1, streamedRows.size())) {
      if (row.getErrorMessage().isPresent()) {
        throw new KsqlStatementException(
            row.getErrorMessage().get().getMessage(),
            statement.getMaskedStatementText()
        );
      }

      if (!row.getRow().isPresent()) {
        throw new KsqlServerException("Unexpected forwarding response");
      }

      rows.add(row.getRow().get().values());
    }

    return new TableRows(
        statement.getMaskedStatementText(),
        header.getQueryId(),
        header.getSchema(),
        rows.build()
    );
  }

  private static QueryId uniqueQueryId() {
    return new QueryId("query_" + System.currentTimeMillis());
  }

  private static ImmutableAnalysis analyze(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext
  ) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(executionContext.getMetaStore(), "");

    return queryAnalyzer.analyze(statement.getStatement(), Optional.empty());
  }

  static final class PullQueryContext {

    private final List<KsqlPartitionLocation> locations;
    private final Materialization mat;
    private final ImmutableAnalysis analysis;
    private final WhereInfo whereInfo;
    private final QueryId queryId;
    private final QueryContext.Stacker contextStacker;
    private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;

    private PullQueryContext(
        final List<KsqlPartitionLocation> locations,
        final Materialization mat,
        final ImmutableAnalysis analysis,
        final WhereInfo whereInfo,
        final QueryId queryId,
        final QueryContext.Stacker contextStacker,
        final Optional<PullQueryExecutorMetrics> pullQueryMetrics
    ) {
      this.locations = Objects.requireNonNull(locations, "locations");
      this.mat = Objects.requireNonNull(mat, "materialization");
      this.analysis = Objects.requireNonNull(analysis, "analysis");
      this.whereInfo = Objects.requireNonNull(whereInfo, "whereInfo");
      this.queryId = Objects.requireNonNull(queryId, "queryId");
      this.contextStacker = Objects.requireNonNull(contextStacker, "contextStacker");
      this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
    }
  }

  private static final class WindowBounds {

    private final Range<Instant> start;
    private final Range<Instant> end;

    private WindowBounds(
        final Range<Instant> start,
        final Range<Instant> end
    ) {
      this.start = Objects.requireNonNull(start, "startBounds");
      this.end = Objects.requireNonNull(end, "endBounds");
    }
  }

  private static final class WhereInfo {

    private final List<Object> keysBound;
    private final Optional<WindowBounds> windowBounds;

    private WhereInfo(
        final List<Object> keysBound,
        final Optional<WindowBounds> windowBounds
    ) {
      this.keysBound = keysBound;
      this.windowBounds = Objects.requireNonNull(windowBounds);
    }
  }

  private static final class Result {

    private final LogicalSchema schema;
    private final List<? extends TableRow> rows;

    private Result(
        final LogicalSchema schema,
        final List<? extends TableRow> rows
    ) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.rows = Objects.requireNonNull(rows, "rows");
    }
  }

  private static WhereInfo extractWhereInfo(
      final ImmutableAnalysis analysis,
      final PersistentQueryMetadata query
  ) {
    final boolean windowed = query.getResultTopic().getKeyFormat().isWindowed();

    final Expression where = analysis.getWhereExpression()
        .orElseThrow(() -> invalidWhereClauseException("Missing WHERE clause", windowed));

    final KeyAndWindowBounds keyAndWindowBounds = extractComparisons(where, query);
    final List<ComparisonExpression> keyComparison = keyAndWindowBounds.getKeyColExpression();
    final List<InPredicate> inPredicate = keyAndWindowBounds.getInPredicate();
    if (keyComparison.size() == 0 && inPredicate.size() == 0) {
      throw invalidWhereClauseException("WHERE clause missing key column", windowed);
    } else if ((keyComparison.size() + inPredicate.size()) > 1) {
      throw invalidWhereClauseException("Multiple bounds on key column", windowed);
    }

    final List<Object> keys;
    if (keyComparison.size() > 0) {
      keys = ImmutableList.of(
          extractKeyWhereClause(keyComparison, windowed, query.getLogicalSchema()));
    } else {
      keys = extractKeysFromInPredicate(inPredicate, windowed, query.getLogicalSchema());
    }

    if (!windowed) {
      if (keyAndWindowBounds.getWindowStartExpression().size() > 0
          || keyAndWindowBounds.getWindowEndExpression().size() > 0) {
        throw invalidWhereClauseException("Unsupported WHERE clause", false);
      }

      return new WhereInfo(keys, Optional.empty());
    }

    final WindowBounds windowBounds =
        extractWhereClauseWindowBounds(keyAndWindowBounds);

    return new WhereInfo(keys, Optional.of(windowBounds));
  }

  private static List<Object> extractKeysFromInPredicate(
      final List<InPredicate> inPredicates,
      final boolean windowed,
      final LogicalSchema schema
  ) {
    final InPredicate inPredicate = Iterables.getLast(inPredicates);
    final List<Object> result = new ArrayList<>();
    for (Expression expression : inPredicate.getValueList().getValues()) {
      if (!(expression instanceof Literal)) {
        throw new KsqlException("Only comparison to literals is currently supported: "
            + inPredicate);
      }
      if (expression instanceof NullLiteral) {
        throw new KsqlException("Primary key columns can not be NULL: " + inPredicate);
      }
      final Object value = ((Literal) expression).getValue();
      result.add(coerceKey(schema, value, windowed));
    }
    return result;
  }

  private static Object extractKeyWhereClause(
      final List<ComparisonExpression> comparisons,
      final boolean windowed,
      final LogicalSchema schema
  ) {
    final ComparisonExpression comparison = Iterables.getLast(comparisons);
    if (comparison.getType() != Type.EQUAL) {
      final ColumnName keyColumn = Iterables.getOnlyElement(schema.key()).name();
      throw invalidWhereClauseException("Bound on '" + keyColumn.text()
          + "' must currently be '='", windowed);
    }

    final Expression other = getNonColumnRefSide(comparison);
    if (!(other instanceof Literal)) {
      throw new KsqlException("Ony comparison to literals is currently supported: " + comparison);
    }

    if (other instanceof NullLiteral) {
      throw new KsqlException("Primary key columns can not be NULL: " + comparison);
    }

    final Object right = ((Literal) other).getValue();
    return coerceKey(schema, right, windowed);
  }

  private static Object coerceKey(
      final LogicalSchema schema,
      final Object right,
      final boolean windowed
  ) {
    if (schema.key().size() != 1) {
      throw invalidWhereClauseException("Only single KEY column supported", windowed);
    }

    final Column keyColumn = schema.key().get(0);

    return DefaultSqlValueCoercer.INSTANCE.coerce(right, keyColumn.type())
        .orElseThrow(() -> new KsqlException("'" + right + "' can not be converted "
            + "to the type of the key column: " + keyColumn.toString(FormatOptions.noEscape())))
        .orElse(null);
  }

  private static WindowBounds extractWhereClauseWindowBounds(
      final KeyAndWindowBounds keyAndWindowBounds
  ) {
    return new WindowBounds(
        extractWhereClauseWindowBounds(ComparisonTarget.WINDOWSTART,
            keyAndWindowBounds.getWindowStartExpression()),
        extractWhereClauseWindowBounds(ComparisonTarget.WINDOWEND,
            keyAndWindowBounds.getWindowEndExpression())
    );
  }

  private static Range<Instant> extractWhereClauseWindowBounds(
      final ComparisonTarget windowType,
      final List<ComparisonExpression> comparisons
  ) {
    if (comparisons.isEmpty()) {
      return Range.all();
    }

    final Map<Type, List<ComparisonExpression>> byType = comparisons.stream()
        .collect(Collectors.groupingBy(PullQueryExecutor::getSimplifiedBoundType));

    final SetView<Type> unsupported = Sets.difference(byType.keySet(), VALID_WINDOW_BOUNDS_TYPES);
    if (!unsupported.isEmpty()) {
      throw invalidWhereClauseException(
          "Unsupported " + windowType + " bounds: " + unsupported, true);
    }

    final String duplicates = byType.entrySet().stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> e.getKey() + ": " + e.getValue())
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw invalidWhereClauseException(
          "Duplicate " + windowType + " bounds on: " + duplicates, true);
    }

    final Map<Type, ComparisonExpression> singles = byType.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get(0)));

    final ComparisonExpression equals = singles.get(Type.EQUAL);
    if (equals != null) {
      if (byType.size() > 1) {
        throw invalidWhereClauseException(
            "`" + equals + "` cannot be combined with other " + windowType + " bounds",
            true
        );
      }

      return Range.singleton(asInstant(getNonColumnRefSide(equals)));
    }

    final Optional<ComparisonExpression> upper =
        Optional.ofNullable(singles.get(Type.LESS_THAN));

    final Optional<ComparisonExpression> lower =
        Optional.ofNullable(singles.get(Type.GREATER_THAN));

    return extractWindowBound(lower, upper);
  }

  private static Type getSimplifiedBoundType(final ComparisonExpression comparison) {
    final Type type = comparison.getType();
    final boolean inverted = comparison.getRight() instanceof UnqualifiedColumnReferenceExp;

    switch (type) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return inverted ? Type.GREATER_THAN : Type.LESS_THAN;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return inverted ? Type.LESS_THAN : Type.GREATER_THAN;
      default:
        return type;
    }
  }

  private static Range<Instant> extractWindowBound(
      final Optional<ComparisonExpression> lowerComparison,
      final Optional<ComparisonExpression> upperComparison
  ) {
    if (!lowerComparison.isPresent() && !upperComparison.isPresent()) {
      return Range.all();
    }

    if (!lowerComparison.isPresent()) {
      final Instant upper = asInstant(getNonColumnRefSide(upperComparison.get()));
      final BoundType upperType = getRangeBoundType(upperComparison.get());
      return Range.upTo(upper, upperType);
    }

    if (!upperComparison.isPresent()) {
      final Instant lower = asInstant(getNonColumnRefSide(lowerComparison.get()));
      final BoundType lowerType = getRangeBoundType(lowerComparison.get());
      return Range.downTo(lower, lowerType);
    }

    final Instant lower = asInstant(getNonColumnRefSide(lowerComparison.get()));
    final BoundType lowerType = getRangeBoundType(lowerComparison.get());

    final Instant upper = asInstant(getNonColumnRefSide(upperComparison.get()));
    final BoundType upperType = getRangeBoundType(upperComparison.get());

    return Range.range(lower, lowerType, upper, upperType);
  }

  private static BoundType getRangeBoundType(final ComparisonExpression lowerComparison) {
    final boolean openBound = lowerComparison.getType() == Type.LESS_THAN
        || lowerComparison.getType() == Type.GREATER_THAN;

    return openBound
        ? BoundType.OPEN
        : BoundType.CLOSED;
  }

  private static Expression getNonColumnRefSide(final ComparisonExpression comparison) {
    return comparison.getRight() instanceof UnqualifiedColumnReferenceExp
        ? comparison.getLeft()
        : comparison.getRight();
  }

  private static Instant asInstant(final Expression other) {
    if (other instanceof IntegerLiteral) {
      return Instant.ofEpochMilli(((IntegerLiteral) other).getValue());
    }

    if (other instanceof LongLiteral) {
      return Instant.ofEpochMilli(((LongLiteral) other).getValue());
    }

    if (other instanceof StringLiteral) {
      final String text = ((StringLiteral) other).getValue();
      try {
        final long timestamp = new PartialStringToTimestampParser()
            .parse(text);

        return Instant.ofEpochMilli(timestamp);
      } catch (final Exception e) {
        throw invalidWhereClauseException("Failed to parse datetime: " + text, true);
      }
    }

    throw invalidWhereClauseException(
        "Window bounds must be an INT, BIGINT or STRING containing a datetime.",
        true
    );
  }

  private enum ComparisonTarget {
    WINDOWSTART,
    WINDOWEND
  }

  private static class KeyAndWindowBounds {
    private List<ComparisonExpression> keyColExpression = new ArrayList<>();
    private List<ComparisonExpression> windowStartExpression = new ArrayList<>();
    private List<ComparisonExpression> windowEndExpression = new ArrayList<>();
    private List<InPredicate> inPredicate = new ArrayList<>();

    KeyAndWindowBounds() {
    }

    public KeyAndWindowBounds addKeyColExpression(final ComparisonExpression keyColExpression) {
      this.keyColExpression.add(keyColExpression);
      return this;
    }

    public KeyAndWindowBounds addWindowStartExpression(
        final ComparisonExpression windowStartExpression) {
      this.windowStartExpression.add(windowStartExpression);
      return this;
    }

    public KeyAndWindowBounds addWindowEndExpression(
        final ComparisonExpression windowEndExpression) {
      this.windowEndExpression.add(windowEndExpression);
      return this;
    }

    public KeyAndWindowBounds addInPredicate(final InPredicate inPredicate) {
      this.inPredicate.add(inPredicate);
      return this;
    }

    public KeyAndWindowBounds merge(final KeyAndWindowBounds other) {
      keyColExpression.addAll(other.keyColExpression);
      windowStartExpression.addAll(other.windowStartExpression);
      windowEndExpression.addAll(other.windowEndExpression);
      inPredicate.addAll(other.inPredicate);
      return this;
    }

    public List<ComparisonExpression> getKeyColExpression() {
      return keyColExpression;
    }

    public List<ComparisonExpression> getWindowStartExpression() {
      return windowStartExpression;
    }

    public List<ComparisonExpression> getWindowEndExpression() {
      return windowEndExpression;
    }

    public List<InPredicate> getInPredicate() {
      return inPredicate;
    }
  }

  private static KeyAndWindowBounds extractComparisons(
      final Expression exp,
      final PersistentQueryMetadata query
  ) {
    if (exp instanceof ComparisonExpression) {
      final ComparisonExpression comparison = (ComparisonExpression) exp;
      return extractWhereClauseTarget(comparison, query);
    }

    if (exp instanceof InPredicate) {
      final InPredicate inPredicate = (InPredicate) exp;
      return extractWhereClauseTarget(inPredicate, query);
    }

    if (exp instanceof LogicalBinaryExpression) {
      final LogicalBinaryExpression binary = (LogicalBinaryExpression) exp;
      if (binary.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + exp, false);
      }

      final KeyAndWindowBounds left = extractComparisons(binary.getLeft(), query);
      final KeyAndWindowBounds right = extractComparisons(binary.getRight(), query);
      return left.merge(right);
    }

    throw invalidWhereClauseException("Unsupported expression: " + exp, false);
  }

  private static KeyAndWindowBounds extractWhereClauseTarget(
      final ComparisonExpression comparison,
      final PersistentQueryMetadata query
  ) {
    final UnqualifiedColumnReferenceExp column;
    if (comparison.getRight() instanceof UnqualifiedColumnReferenceExp) {
      column = (UnqualifiedColumnReferenceExp) comparison.getRight();
    } else if (comparison.getLeft() instanceof UnqualifiedColumnReferenceExp) {
      column = (UnqualifiedColumnReferenceExp) comparison.getLeft();
    } else {
      throw invalidWhereClauseException("Invalid WHERE clause: " + comparison, false);
    }

    final ColumnName columnName = column.getColumnName();
    if (columnName.equals(SystemColumns.WINDOWSTART_NAME)) {
      return new KeyAndWindowBounds().addWindowStartExpression(comparison);
    }

    if (columnName.equals(SystemColumns.WINDOWEND_NAME)) {
      return new KeyAndWindowBounds().addWindowEndExpression(comparison);
    }

    final ColumnName keyColumn = Iterables.getOnlyElement(query.getLogicalSchema().key()).name();
    if (columnName.equals(keyColumn)) {
      return new KeyAndWindowBounds().addKeyColExpression(comparison);
    }

    throw invalidWhereClauseException(
        "WHERE clause on unsupported column: " + columnName.text(),
        false
    );
  }

  private static KeyAndWindowBounds extractWhereClauseTarget(
      final InPredicate inPredicate,
      final PersistentQueryMetadata query
  ) {
    final UnqualifiedColumnReferenceExp column
        = (UnqualifiedColumnReferenceExp) inPredicate.getValue();
    final ColumnName keyColumn = Iterables.getOnlyElement(query.getLogicalSchema().key()).name();
    if (column.getColumnName().equals(keyColumn)) {
      return new KeyAndWindowBounds().addInPredicate(inPredicate);
    }

    throw invalidWhereClauseException(
        "IN expression on unsupported column: " + column.getColumnName().text(),
        false
    );
  }

  private static boolean isSelectStar(final Select select) {
    final boolean someStars = select.getSelectItems().stream()
        .anyMatch(s -> s instanceof AllColumns);

    if (someStars && select.getSelectItems().size() != 1) {
      throw new KsqlException("Pull queries only support wildcards in the projects "
          + "if they are the only expression");
    }

    return someStars;
  }

  private static List<List<?>> handleSelects(
      final Result input,
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ImmutableAnalysis analysis,
      final LogicalSchema outputSchema,
      final List<SelectExpression> projection,
      final Optional<WindowType> windowType,
      final QueryId queryId,
      final Stacker contextStacker
  ) {
    final boolean noSystemColumns = analysis.getSelectColumnNames().stream()
        .noneMatch(SystemColumns::isSystemColumn);

    final boolean noKeyColumns = analysis.getSelectColumnNames().stream()
        .noneMatch(input.schema::isKeyColumn);

    final LogicalSchema intermediateSchema;
    final Function<TableRow, GenericRow> preSelectTransform;
    if (noSystemColumns && noKeyColumns) {
      intermediateSchema = input.schema;
      preSelectTransform = TableRow::value;
    } else {
      // SelectValueMapper requires the rowTime & key fields in the value schema :(
      final boolean windowed = windowType.isPresent();

      intermediateSchema = input.schema
          .withPseudoAndKeyColsInValue(windowed);

      preSelectTransform = row -> {
        final Struct key = row.key();
        final GenericRow value = row.value();

        final List<Object> keyFields = key.schema().fields().stream()
            .map(key::get)
            .collect(Collectors.toList());

        value.ensureAdditionalCapacity(
            1 // ROWTIME
            + keyFields.size()
            + row.window().map(w -> 2).orElse(0)
        );

        value.append(row.rowTime());
        value.appendAll(keyFields);

        row.window().ifPresent(window -> {
          value.append(window.start().toEpochMilli());
          value.append(window.end().toEpochMilli());
        });

        return value;
      };
    }

    final KsqlConfig ksqlConfig = statement.getSessionConfig().getConfig(true);

    final SelectValueMapper<Object> select = SelectValueMapperFactory.create(
        projection,
        intermediateSchema,
        ksqlConfig,
        executionContext.getMetaStore()
    );

    final ProcessingLogger logger = executionContext
        .getProcessingLogContext()
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                QueryType.PULL_QUERY, contextStacker.push("PROJECT").getQueryContext())
        );

    final KsqlTransformer<Object, GenericRow> transformer = select
        .getTransformer(logger);

    final ImmutableList.Builder<List<?>> output = ImmutableList.builder();
    input.rows.forEach(r -> {
      final GenericRow intermediate = preSelectTransform.apply(r);

      final GenericRow mapped = transformer.transform(
          r.key(),
          intermediate,
          new PullProcessingContext(r.rowTime())
      );
      validateProjection(mapped, outputSchema);
      output.add(mapped.values());
    });

    return output.build();
  }

  private static void validateProjection(
      final GenericRow fullRow,
      final LogicalSchema schema
  ) {
    final int actual = fullRow.size();
    final int expected = schema.columns().size();
    if (actual != expected) {
      throw new IllegalStateException("Row column count mismatch."
          + " expected:" + expected
          + ", got:" + actual
      );
    }
  }

  private static LogicalSchema selectOutputSchema(
      final Result input,
      final KsqlExecutionContext executionContext,
      final List<SelectExpression> selectExpressions,
      final Optional<WindowType> windowType
  ) {
    final Builder schemaBuilder = LogicalSchema.builder();

    // Copy meta & key columns into the value schema as SelectValueMapper expects it:
    final LogicalSchema schema = input.schema
        .withPseudoAndKeyColsInValue(windowType.isPresent());

    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, executionContext.getMetaStore());

    for (final SelectExpression select : selectExpressions) {
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());

      if (input.schema.isKeyColumn(select.getAlias())
          || select.getAlias().equals(SystemColumns.WINDOWSTART_NAME)
          || select.getAlias().equals(SystemColumns.WINDOWEND_NAME)
      ) {
        schemaBuilder.keyColumn(select.getAlias(), type);
      } else {
        schemaBuilder.valueColumn(select.getAlias(), type);
      }
    }
    return schemaBuilder.build();
  }

  private static PersistentQueryMetadata findMaterializingQuery(
      final KsqlExecutionContext executionContext,
      final ImmutableAnalysis analysis
  ) {
    final MetaStore metaStore = executionContext.getMetaStore();

    final SourceName sourceName = getSourceName(analysis);

    final Set<String> queries = metaStore.getQueriesWithSink(sourceName);
    if (queries.isEmpty()) {
      throw notMaterializedException(sourceName);
    }
    if (queries.size() > 1) {
      throw new KsqlException("Multiple queries currently materialize '" + sourceName + "'."
          + " KSQL currently only supports pull queries when the table has only been"
          + " materialized once.");
    }

    final QueryId queryId = new QueryId(Iterables.get(queries, 0));

    final PersistentQueryMetadata query = executionContext
        .getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Materializing query has been stopped"));

    if (query.getDataSourceType() != DataSourceType.KTABLE) {
      throw new KsqlException("Pull queries are not supported on streams.");
    }

    return query;
  }

  private static SourceName getSourceName(final ImmutableAnalysis analysis) {
    final DataSource source = analysis.getFrom().getDataSource();
    return source.getName();
  }

  private static KsqlException notMaterializedException(final SourceName sourceTable) {
    return new KsqlException(
        "Can't pull from " + sourceTable + " as it's not a materialized table."
        + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
    );
  }

  private static KsqlException invalidWhereClauseException(
      final String msg,
      final boolean windowed
  ) {
    final String additional = !windowed
        ? ""
        : System.lineSeparator()
            + " - (optionally) limits the time bounds of the windowed table."
            + System.lineSeparator()
            + "\t Bounds on " + VALID_WINDOW_BOUNDS_COLUMNS + " are supported"
            + System.lineSeparator()
            + "\t Supported operators are " + VALID_WINDOW_BOUNDS_TYPES_STRING;

    return new KsqlException(msg + ". "
        + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
        + System.lineSeparator()
        + "Pull queries require a WHERE clause that:"
        + System.lineSeparator()
        + " - limits the query to a single key, e.g. `SELECT * FROM X WHERE <key-column>=Y;`."
        + additional
    );
  }

  private static Struct asKeyStruct(final Object keyValue, final PhysicalSchema physicalSchema) {
    final ConnectSchema keySchema = ConnectSchemas
        .columnsToConnectSchema(physicalSchema.keySchema().columns());

    final Field keyField = Iterables.getOnlyElement(keySchema.fields());

    final Struct key = new Struct(keySchema);
    key.put(keyField, keyValue);
    return key;
  }

  private static final class ColumnReferenceRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private ColumnReferenceRewriter() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context<Void> ctx
    ) {
      return Optional.of(new UnqualifiedColumnReferenceExp(node.getColumnName()));
    }
  }

  private static final class ConfigRoutingOptions implements RoutingOptions {

    private final KsqlConfig ksqlConfig;
    private final Map<String, ?> configOverrides;
    private final Map<String, ?> requestProperties;

    ConfigRoutingOptions(
        final KsqlConfig ksqlConfig,
        final Map<String, ?> configOverrides,
        final Map<String, ?> requestProperties
    ) {
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
      this.configOverrides = configOverrides;
      this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties");
    }

    private long getLong(final String key) {
      if (configOverrides.containsKey(key)) {
        return (Long) configOverrides.get(key);
      }
      return ksqlConfig.getLong(key);
    }

    private boolean getForwardedFlag(final String key) {
      if (requestProperties.containsKey(key)) {
        return (Boolean) requestProperties.get(key);
      }
      return KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DEFAULT;
    }

    public boolean isDebugRequest() {
      if (requestProperties.containsKey(KsqlRequestConfig.KSQL_DEBUG_REQUEST)) {
        return (Boolean) requestProperties.get(KsqlRequestConfig.KSQL_DEBUG_REQUEST);
      }
      return KsqlRequestConfig.KSQL_DEBUG_REQUEST_DEFAULT;
    }

    @Override
    public Set<Integer> getPartitions() {
      if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS)) {
        @SuppressWarnings("unchecked")
        final List<String> partitions = (List<String>) requestProperties.get(
            KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS);
        return partitions.stream()
            .map(partition -> {
              try {
                return Integer.parseInt(partition);
              } catch (NumberFormatException e) {
                throw new IllegalStateException("Internal request got a bad partition "
                    + partition);
              }
            }).collect(Collectors.toSet());
      }
      return Collections.emptySet();
    }

    @Override
    public long getOffsetLagAllowed() {
      return getLong(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG);
    }

    @Override
    public boolean skipForwardRequest() {
      return getForwardedFlag(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING);
    }
  }
}