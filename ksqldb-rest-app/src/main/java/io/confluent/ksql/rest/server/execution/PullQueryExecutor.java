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
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.context.QueryLoggerUtil.QueryType;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
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
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  public PullQueryExecutor(
      final KsqlExecutionContext executionContext,
      final RoutingFilterFactory routingFilterFactory,
      final KsqlConfig ksqlConfig
  ) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
    this.routingFilterFactory =
        Objects.requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = RateLimiter.create(ksqlConfig.getInt(
        KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG));
  }

  @SuppressWarnings("unused") // Needs to match validator API.
  public static void validate(
      final ConfiguredStatement<Query> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    throw new KsqlRestException(Errors.queryEndpoint(statement.getStatementText()));
  }

  public PullQueryResult execute(
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final Optional<Boolean> isInternalRequest
  ) {
    if (!statement.getStatement().isPullQuery()) {
      throw new IllegalArgumentException("Executor can only handle pull queries");
    }

    if (!statement.getConfig().getBoolean(KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG)) {
      throw new KsqlException(
          "Pull queries are disabled."
              + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
              + System.lineSeparator()
              + "Please set " + KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG + "=true to enable "
              + "this feature.");
    }

    try {
      final RoutingOptions routingOptions = new ConfigRoutingOptions(
          statement.getConfig(), statement.getConfigOverrides(), statement.getRequestProperties());
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

      final Struct key = asKeyStruct(whereInfo.keyBound, query.getPhysicalSchema());

      final PullQueryContext pullQueryContext = new PullQueryContext(
          key,
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
          pullQueryContext,
          routingOptions
      );
    } catch (final Exception e) {
      pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1));
      throw new KsqlStatementException(
          e.getMessage() == null ? "Server Error" : e.getMessage(),
          statement.getStatementText(),
          e
      );
    }
  }

  @VisibleForTesting
  void checkRateLimit() {
    if (!rateLimiter.tryAcquire()) {
      throw new KsqlException("Host is at rate limit for pull queries. Currently set to "
          + rateLimiter.getRate() + " qps.");
    }
  }

  private PullQueryResult handlePullQuery(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final PullQueryContext pullQueryContext,
      final RoutingOptions routingOptions
  ) {
    // Get active and standby nodes for this key
    final Locator locator = pullQueryContext.mat.locator();
    final List<KsqlNode> filteredAndOrderedNodes = locator.locate(
        pullQueryContext.key,
        routingOptions,
        routingFilterFactory
    );

    if (filteredAndOrderedNodes.isEmpty()) {
      LOG.debug("Unable to execute pull query: {}. All nodes are dead or exceed max allowed lag.",
                statement.getStatementText());
      throw new MaterializationException(String.format(
          "Unable to execute pull query %s. All nodes are dead or exceed max allowed lag.",
          statement.getStatementText()));
    }

    // Nodes are ordered by preference: active is first if alive then standby nodes in
    // increasing order of lag.
    for (KsqlNode node : filteredAndOrderedNodes) {
      try {
        final Optional<KsqlNode> debugNode = Optional.ofNullable(
            routingOptions.isDebugRequest() ? node : null);
        return new PullQueryResult(
            routeQuery(node, statement, executionContext, serviceContext, pullQueryContext),
            debugNode);
      } catch (Exception t) {
        LOG.debug("Error routing query {} to host {} at timestamp {} with exception {}",
                  statement.getStatementText(), node, System.currentTimeMillis(), t);
      }
    }
    throw new MaterializationException(String.format(
        "Unable to execute pull query: %s", statement.getStatementText()));
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
               statement.getStatementText(), node.location(), System.currentTimeMillis());
      pullQueryContext.pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordLocalRequests(1));
      return queryRowsLocally(
          statement,
          executionContext,
          pullQueryContext);
    } else {
      LOG.debug("Query {} routed to host {} at timestamp {}.",
                statement.getStatementText(), node.location(), System.currentTimeMillis());
      pullQueryContext.pullQueryMetrics
          .ifPresent(queryExecutorMetrics -> queryExecutorMetrics.recordRemoteRequests(1));
      return forwardTo(node, statement, serviceContext);
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

      final List<? extends TableRow> rows = pullQueryContext.mat.windowed()
          .get(pullQueryContext.key, windowBounds.start, windowBounds.end);

      result = new Result(pullQueryContext.mat.schema(), rows);
    } else {
      final List<? extends TableRow> rows = pullQueryContext.mat.nonWindowed()
          .get(pullQueryContext.key)
          .map(ImmutableList::of)
          .orElse(ImmutableList.of());

      result = new Result(pullQueryContext.mat.schema(), rows);
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
        statement.getStatementText(),
        pullQueryContext.queryId,
        outputSchema,
        rows
    );
  }

  private static TableRows forwardTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext
  ) {
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);
    final RestResponse<List<StreamedRow>> response = serviceContext
        .getKsqlClient()
        .makeQueryRequest(
            owner.location(),
            statement.getStatementText(),
            statement.getConfigOverrides(),
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
            statement.getStatementText()
        );
      }

      if (!row.getRow().isPresent()) {
        throw new KsqlServerException("Unexpected forwarding response");
      }

      rows.add(row.getRow().get().values());
    }

    return new TableRows(
        statement.getStatementText(),
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

  private static final class PullQueryContext {

    private final Struct key;
    private final Materialization mat;
    private final ImmutableAnalysis analysis;
    private final WhereInfo whereInfo;
    private final QueryId queryId;
    private final QueryContext.Stacker contextStacker;
    private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;

    private PullQueryContext(
        final Struct key,
        final Materialization mat,
        final ImmutableAnalysis analysis,
        final WhereInfo whereInfo,
        final QueryId queryId,
        final QueryContext.Stacker contextStacker,
        final Optional<PullQueryExecutorMetrics> pullQueryMetrics

    ) {
      this.key = Objects.requireNonNull(key, "key");
      this.mat = Objects.requireNonNull(mat, "materialization");
      this.analysis = Objects.requireNonNull(analysis, "analysis");
      this.whereInfo = Objects.requireNonNull(whereInfo, "whereInfo");
      this.queryId = Objects.requireNonNull(queryId, "queryId");
      this.contextStacker = Objects.requireNonNull(contextStacker, "contextStacker");
      this.pullQueryMetrics = Objects.requireNonNull(
          pullQueryMetrics, "pullQueryExecutorMetrics");
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

    private final Object keyBound;
    private final Optional<WindowBounds> windowBounds;

    private WhereInfo(
        final Object keyBound,
        final Optional<WindowBounds> windowBounds
    ) {
      this.keyBound = keyBound;
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

    final Map<ComparisonTarget, List<ComparisonExpression>> comparisons =
        extractComparisons(where, query);

    final List<ComparisonExpression> keyComparison = comparisons.get(ComparisonTarget.KEYCOL);
    if (keyComparison == null) {
      throw invalidWhereClauseException("WHERE clause missing key column", windowed);
    }

    final Object key = extractKeyWhereClause(
        keyComparison,
        windowed,
        query.getLogicalSchema()
    );

    if (!windowed) {
      if (comparisons.size() > 1) {
        throw invalidWhereClauseException("Unsupported WHERE clause", false);
      }

      return new WhereInfo(key, Optional.empty());
    }

    final WindowBounds windowBounds =
        extractWhereClauseWindowBounds(comparisons);

    return new WhereInfo(key, Optional.of(windowBounds));
  }

  private static Object extractKeyWhereClause(
      final List<ComparisonExpression> comparisons,
      final boolean windowed,
      final LogicalSchema schema
  ) {
    if (comparisons.size() != 1) {
      throw invalidWhereClauseException("Multiple bounds on key column", windowed);
    }

    final ComparisonExpression comparison = comparisons.get(0);
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
      final Map<ComparisonTarget, List<ComparisonExpression>> allComparisons
  ) {
    return new WindowBounds(
        extractWhereClauseWindowBounds(ComparisonTarget.WINDOWSTART, allComparisons),
        extractWhereClauseWindowBounds(ComparisonTarget.WINDOWEND, allComparisons)
    );
  }

  private static Range<Instant> extractWhereClauseWindowBounds(
      final ComparisonTarget windowType,
      final Map<ComparisonTarget, List<ComparisonExpression>> allComparisons
  ) {
    final List<ComparisonExpression> comparisons =
        Optional.ofNullable(allComparisons.get(windowType))
            .orElseGet(ImmutableList::of);

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
    KEYCOL,
    WINDOWSTART,
    WINDOWEND
  }

  private static Map<ComparisonTarget, List<ComparisonExpression>> extractComparisons(
      final Expression exp,
      final PersistentQueryMetadata query
  ) {
    if (exp instanceof ComparisonExpression) {
      final ComparisonExpression comparison = (ComparisonExpression) exp;
      final ComparisonTarget target = extractWhereClauseTarget(comparison, query);
      return ImmutableMap.of(target, ImmutableList.of(comparison));
    }

    if (exp instanceof LogicalBinaryExpression) {
      final LogicalBinaryExpression binary = (LogicalBinaryExpression) exp;
      if (binary.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + exp, false);
      }

      final Map<ComparisonTarget, List<ComparisonExpression>> left =
          extractComparisons(binary.getLeft(), query);

      final Map<ComparisonTarget, List<ComparisonExpression>> right =
          extractComparisons(binary.getRight(), query);

      return Stream
          .concat(left.entrySet().stream(), right.entrySet().stream())
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (l, r) ->
              ImmutableList.<ComparisonExpression>builder().addAll(l).addAll(r).build()
          ));
    }

    throw invalidWhereClauseException("Unsupported expression: " + exp, false);
  }

  private static ComparisonTarget extractWhereClauseTarget(
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
      return ComparisonTarget.WINDOWSTART;
    }

    if (columnName.equals(SystemColumns.WINDOWEND_NAME)) {
      return ComparisonTarget.WINDOWEND;
    }

    final ColumnName keyColumn = Iterables.getOnlyElement(query.getLogicalSchema().key()).name();
    if (columnName.equals(keyColumn)) {
      return ComparisonTarget.KEYCOL;
    }

    throw invalidWhereClauseException(
        "WHERE clause on unsupported column: " + columnName.text(),
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

    final KsqlConfig ksqlConfig = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getConfigOverrides());

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
    final Field keyField = Iterables
        .getOnlyElement(physicalSchema.keySchema().ksqlSchema().fields());

    final Struct key = new Struct(physicalSchema.keySchema().ksqlSchema());
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
      this.ksqlConfig = ksqlConfig;
      this.configOverrides = configOverrides;
      this.requestProperties = requestProperties;
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
    public long getOffsetLagAllowed() {
      return getLong(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG);
    }

    @Override
    public boolean skipForwardRequest() {
      return getForwardedFlag(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING);
    }
  }
}