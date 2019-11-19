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

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.SelectValueMapper;
import io.confluent.ksql.execution.streams.SelectValueMapperFactory;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.entity.TableRowsEntityFactory;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class PullQueryExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Set<Type> VALID_WINDOW_BOUNDS_TYPES = ImmutableSet.of(
      Type.EQUAL,
      Type.GREATER_THAN,
      Type.GREATER_THAN_OR_EQUAL,
      Type.LESS_THAN,
      Type.LESS_THAN_OR_EQUAL
  );

  private static final String VALID_WINDOW_BOUNDS_TYPES_STRING =
      VALID_WINDOW_BOUNDS_TYPES.toString();

  private PullQueryExecutor() {
  }

  public static void validate(
      final ConfiguredStatement<Query> statement,
      final Map<String, ?> sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    throw new KsqlRestException(Errors.queryEndpoint(statement.getStatementText()));
  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<Query> statement,
      final Map<String, ?> sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return Optional.of(execute(statement, executionContext, serviceContext));
  }

  public static TableRowsEntity execute(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    if (!statement.getStatement().isPullQuery()) {
      throw new IllegalArgumentException("Executor can only handle pull queries");
    }

    if (!statement.getConfig().getBoolean(KsqlConfig.KSQL_QUERY_PULL_ENABLE_CONFIG)) {
      throw new KsqlRestException(
          Errors.badStatement(
              "Pull queries are disabled. "
                  + PullQueryValidator.NEW_QUERY_SYNTAX_SHORT_HELP
                  + System.lineSeparator()
                  + "Please set " + KsqlConfig.KSQL_QUERY_PULL_ENABLE_CONFIG + "=true to enable "
                  + "this feature.",
              statement.getStatementText()));
    }

    try {
      final Analysis analysis = analyze(statement, executionContext);

      final PersistentQueryMetadata query = findMaterializingQuery(executionContext, analysis);

      final WhereInfo whereInfo = extractWhereInfo(analysis, query);

      final QueryId queryId = PersistentQueryMetadata.getPullQueryId(
          getSourceName(analysis).name());

      final Materialization mat = query
          .getMaterialization()
          .orElseThrow(() -> notMaterializedException(getSourceName(analysis)));

      final Struct rowKey = asKeyStruct(whereInfo.rowkey, query.getPhysicalSchema());

      final KsqlConfig ksqlConfig = statement.getConfig();
      final KsqlNode owner = getOwner(ksqlConfig, rowKey, mat);
      if (!owner.isLocal()) {
        return proxyTo(owner, statement, serviceContext);
      }

      final Result result;
      if (whereInfo.windowStartBounds.isPresent()) {
        final Range<Instant> windowStart = whereInfo.windowStartBounds.get();

        final List<? extends TableRow> rows = mat.windowed()
            .get(rowKey, windowStart);

        result = new Result(mat.schema(), rows);
      } else {
        final List<? extends TableRow> rows = mat.nonWindowed()
            .get(rowKey)
            .map(ImmutableList::of)
            .orElse(ImmutableList.of());

        result = new Result(mat.schema(), rows);
      }

      final LogicalSchema outputSchema;
      final List<List<?>> rows;
      if (isSelectStar(statement.getStatement().getSelect())) {
        outputSchema = TableRowsEntityFactory.buildSchema(result.schema, mat.windowType());
        rows = TableRowsEntityFactory.createRows(result.rows);
      } else {
        final LogicalSchema.Builder schemaBuilder =
            selectSchemaBuilder(result, executionContext, analysis);

        outputSchema = schemaBuilder.build();

        rows = handleSelects(result, statement, executionContext, analysis, outputSchema);
      }

      return new TableRowsEntity(
          statement.getStatementText(),
          queryId,
          outputSchema,
          rows
      );
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage() == null ? "Server Error" : e.getMessage(),
          statement.getStatementText(),
          e
      );
    }
  }

  private static Analysis analyze(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext
  ) {
    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(
        executionContext.getMetaStore(),
        "",
        SerdeOption.none()
    );

    return queryAnalyzer.analyze(statement.getStatement(), Optional.empty());
  }

  private static final class WhereInfo {

    private final Object rowkey;
    private final Optional<Range<Instant>> windowStartBounds;

    private WhereInfo(
        final Object rowkey,
        final Optional<Range<Instant>> windowStartBounds
    ) {
      this.rowkey = rowkey;
      this.windowStartBounds = windowStartBounds;
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
      final Analysis analysis,
      final PersistentQueryMetadata query
  ) {
    final boolean windowed = query.getResultTopic().getKeyFormat().isWindowed();

    final Expression where = analysis.getWhereExpression()
        .orElseThrow(() -> invalidWhereClauseException("Missing WHERE clause", windowed));

    final Map<ComparisonTarget, List<ComparisonExpression>> comparisons = extractComparisons(where);

    final List<ComparisonExpression> rowKeyComparison = comparisons.get(ComparisonTarget.ROWKEY);
    if (rowKeyComparison == null) {
      throw invalidWhereClauseException("WHERE clause missing ROWKEY", windowed);
    }

    final Object rowKey = extractRowKeyWhereClause(rowKeyComparison, windowed);

    if (!windowed) {
      if (comparisons.size() > 1) {
        throw invalidWhereClauseException("Unsupported WHERE clause", false);
      }

      return new WhereInfo(rowKey, Optional.empty());
    }

    final Optional<List<ComparisonExpression>> windowBoundsComparison =
        Optional.ofNullable(comparisons.get(ComparisonTarget.WINDOWSTART));

    final Range<Instant> windowStart = extractWhereClauseWindowBounds(windowBoundsComparison);

    return new WhereInfo(rowKey, Optional.of(windowStart));
  }

  private static Object extractRowKeyWhereClause(
      final List<ComparisonExpression> comparisons,
      final boolean windowed
  ) {
    if (comparisons.size() != 1) {
      throw invalidWhereClauseException("Multiple bounds on ROWKEY", windowed);
    }

    final ComparisonExpression comparison = comparisons.get(0);

    final Expression other = getNonColumnRefSide(comparison);

    if (!(other instanceof StringLiteral)) {
      throw invalidWhereClauseException("ROWKEY must be compared to STRING literal", false);
    }

    if (comparison.getType() != Type.EQUAL) {
      throw invalidWhereClauseException("ROWKEY bound must currently be '='", false);
    }

    final Literal right = (Literal) other;
    return right.getValue();
  }

  private static Range<Instant> extractWhereClauseWindowBounds(
      final Optional<List<ComparisonExpression>> maybeComparisons
  ) {
    if (!maybeComparisons.isPresent()) {
      return Range.all();
    }

    final List<ComparisonExpression> comparisons = maybeComparisons.get();

    final Map<Type, List<ComparisonExpression>> byType = comparisons.stream()
        .collect(Collectors.groupingBy(PullQueryExecutor::getSimplifiedBoundType));

    final SetView<Type> unsupported = Sets.difference(byType.keySet(), VALID_WINDOW_BOUNDS_TYPES);
    if (!unsupported.isEmpty()) {
      throw invalidWhereClauseException(
          "Unsupported " + ComparisonTarget.WINDOWSTART + " bounds: " + unsupported,
          true
      );
    }

    final String duplicates = byType.entrySet().stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> e.getKey() + ": " + e.getValue())
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw invalidWhereClauseException(
          "Duplicate bounds on " + ComparisonTarget.WINDOWSTART + ": " + duplicates,
          true
      );
    }

    final Map<Type, ComparisonExpression> singles = byType.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get(0)));

    final ComparisonExpression equals = singles.get(Type.EQUAL);
    if (equals != null) {
      if (byType.size() > 1) {
        throw invalidWhereClauseException(
            "`" + equals + "` cannot be combined with other bounds on "
                + ComparisonTarget.WINDOWSTART,
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
    final boolean inverted = comparison.getRight() instanceof ColumnReferenceExp;

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
    return comparison.getRight() instanceof ColumnReferenceExp
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
        ComparisonTarget.WINDOWSTART + " bounds must be BIGINT",
        true
    );
  }

  private enum ComparisonTarget {
    ROWKEY,
    WINDOWSTART
  }

  private static Map<ComparisonTarget, List<ComparisonExpression>> extractComparisons(
      final Expression exp
  ) {
    if (exp instanceof ComparisonExpression) {
      final ComparisonExpression comparison = (ComparisonExpression) exp;
      final ComparisonTarget target = extractWhereClauseTarget(comparison);
      return ImmutableMap.of(target, ImmutableList.of(comparison));
    }

    if (exp instanceof LogicalBinaryExpression) {
      final LogicalBinaryExpression binary = (LogicalBinaryExpression) exp;
      if (binary.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + exp, false);
      }

      final Map<ComparisonTarget, List<ComparisonExpression>> left =
          extractComparisons(binary.getLeft());

      final Map<ComparisonTarget, List<ComparisonExpression>> right =
          extractComparisons(binary.getRight());

      return Stream
          .concat(left.entrySet().stream(), right.entrySet().stream())
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (l, r) ->
              ImmutableList.<ComparisonExpression>builder().addAll(l).addAll(r).build()
          ));
    }

    throw invalidWhereClauseException("Unsupported expression: " + exp, false);
  }

  private static ComparisonTarget extractWhereClauseTarget(final ComparisonExpression comparison) {
    final ColumnReferenceExp column;
    if (comparison.getRight() instanceof ColumnReferenceExp) {
      column = (ColumnReferenceExp) comparison.getRight();
    } else if (comparison.getLeft() instanceof ColumnReferenceExp) {
      column = (ColumnReferenceExp) comparison.getLeft();
    } else {
      throw invalidWhereClauseException("Invalid WHERE clause: " + comparison, false);
    }

    final String fieldName = column.getReference().name().toString(FormatOptions.noEscape());

    try {
      return ComparisonTarget.valueOf(fieldName.toUpperCase());
    } catch (final Exception e) {
      throw invalidWhereClauseException("WHERE clause on unsupported field: " + fieldName, false);
    }
  }

  private static boolean isSelectStar(final Select select) {
    final List<SelectItem> selects = select.getSelectItems();
    return selects.size() == 1 && selects.get(0) instanceof AllColumns;
  }

  private static List<List<?>> handleSelects(
      final Result input,
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final Analysis analysis,
      final LogicalSchema outputSchema
  ) {
    final LogicalSchema intermediateSchema;
    final BiFunction<Struct, GenericRow, GenericRow> preSelectTransform;
    if (outputSchema.key().isEmpty()) {
      intermediateSchema = input.schema;
      preSelectTransform = (key, value) -> value;
    } else {
      // SelectValueMapper requires the key fields in the value schema :(
      intermediateSchema = LogicalSchema.builder()
          .keyColumns(input.schema.key())
          .valueColumns(input.schema.value())
          .valueColumns(input.schema.key())
          .build();

      preSelectTransform = (key, value) -> {
        key.schema().fields().forEach(f -> {
          final Object keyField = key.get(f);
          value.getColumns().add(keyField);
        });
        return value;
      };
    }

    final SourceName sourceName = getSourceName(analysis);

    final KsqlConfig ksqlConfig = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    final SelectValueMapper<Object> select = SelectValueMapperFactory.create(
        analysis.getSelectExpressions(),
        intermediateSchema.withAlias(sourceName),
        ksqlConfig,
        executionContext.getMetaStore(),
        NoopProcessingLogContext.INSTANCE.getLoggerFactory().getLogger("any")
    );

    final ImmutableList.Builder<List<?>> output = ImmutableList.builder();
    input.rows.forEach(r -> {
      final GenericRow intermediate = preSelectTransform.apply(r.key(), r.value());
      final GenericRow mapped = select.transform(r.key(), intermediate);
      validateProjection(mapped, outputSchema);
      output.add(mapped.getColumns());
    });

    return output.build();
  }

  private static void validateProjection(
      final GenericRow fullRow,
      final LogicalSchema schema
  ) {
    final int actual = fullRow.getColumns().size();
    final int expected = schema.columns().size();
    if (actual != expected) {
      throw new IllegalStateException("Row column count mismatch."
          + " expected:" + expected
          + ", got:" + actual
      );
    }
  }

  private static LogicalSchema.Builder selectSchemaBuilder(
      final Result input,
      final KsqlExecutionContext executionContext,
      final Analysis analysis
  ) {
    final Builder schemaBuilder = LogicalSchema.builder()
        .noImplicitColumns();

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        input.schema.withAlias(analysis.getFromDataSources().get(0).getAlias()),
        executionContext.getMetaStore()
    );

    for (int idx = 0; idx < analysis.getSelectExpressions().size(); idx++) {
      final SelectExpression select = analysis.getSelectExpressions().get(idx);
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());

      if (input.schema.isKeyColumn(select.getAlias())) {
        schemaBuilder.keyColumn(select.getAlias(), type);
      } else {
        schemaBuilder.valueColumn(select.getAlias(), type);
      }
    }
    return schemaBuilder;
  }

  private static PersistentQueryMetadata findMaterializingQuery(
      final KsqlExecutionContext executionContext,
      final Analysis analysis
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

    return executionContext.getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Materializing query has been stopped"));
  }

  private static SourceName getSourceName(final Analysis analysis) {
    final DataSource<?> source = analysis.getFromDataSources().get(0).getDataSource();
    return source.getName();
  }

  private static KsqlNode getOwner(
      final KsqlConfig ksqlConfig,
      final Struct rowKey,
      final Materialization mat
  ) {
    final Locator locator = mat.locator();

    final long timeoutMs =
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_CONFIG);
    final long threshold = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < threshold) {
      final Optional<KsqlNode> owner = locator.locate(rowKey);
      if (owner.isPresent()) {
        return owner.get();
      }
    }

    throw new MaterializationTimeOutException(
        "The owner of the key could not be determined within the configured timeout: "
            + timeoutMs + "ms, config: " + KsqlConfig.KSQL_QUERY_PULL_ROUTING_TIMEOUT_MS_CONFIG
    );
  }

  private static TableRowsEntity proxyTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext
  ) {
    final RestResponse<List<StreamedRow>> response = serviceContext
        .getKsqlClient()
        .makeQueryRequest(owner.location(), statement.getStatementText());

    if (response.isErroneous()) {
      throw new KsqlServerException("Proxy attempt failed: " + response.getErrorMessage());
    }

    final List<StreamedRow> streamedRows = response.getResponse();
    if (streamedRows.isEmpty()) {
      throw new KsqlServerException("Invalid empty response from proxy call");
    }

    // Temporary code to convert from QueryStream to TableRowsEntity
    // Tracked by: https://github.com/confluentinc/ksql/issues/3865
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
        throw new KsqlServerException("Unexpected proxy response");
      }

      rows.add(row.getRow().get().getColumns());
    }

    return new TableRowsEntity(
        statement.getStatementText(),
        header.getQueryId(),
        header.getSchema(),
        rows.build()
    );
  }

  private static KsqlException notMaterializedException(final SourceName sourceTable) {
    return new KsqlException("'"
        + sourceTable.toString(FormatOptions.noEscape()) + "' is not materialized. "
        + PullQueryValidator.NEW_QUERY_SYNTAX_SHORT_HELP
        + System.lineSeparator()
        + " KSQL currently only supports pull queries on materialized aggregate tables."
        + " i.e. those created by a 'CREATE TABLE AS SELECT <fields>, <aggregate_functions> "
        + "FROM <sources> GROUP BY <key>' style statement."
        + System.lineSeparator()
        + PullQueryValidator.NEW_QUERY_SYNTAX_ADDITIONAL_HELP
    );
  }

  private static KsqlException invalidWhereClauseException(
      final String msg,
      final boolean windowed
  ) {
    final String additional = !windowed
        ? ""
        : System.lineSeparator()
            + " - limits the time bounds of the windowed table. This can be: "
            + System.lineSeparator()
            + "    + a single window lower bound, e.g. `WHERE WINDOWSTART = z`, or"
            + System.lineSeparator()
            + "    + a range, e.g. `WHERE a <= WINDOWSTART AND WINDOWSTART < b"
            + System.lineSeparator()
            + "WINDOWSTART currently supports operators: " + VALID_WINDOW_BOUNDS_TYPES_STRING
            + System.lineSeparator()
            + "WINDOWSTART currently comparison with epoch milliseconds "
            + "or a datetime string in the form: " + KsqlConstants.DATE_TIME_PATTERN
            + " with an optional numeric 4-digit timezone, e.g. '+0100'";

    return new KsqlException(msg + ". "
        + PullQueryValidator.NEW_QUERY_SYNTAX_SHORT_HELP
        + System.lineSeparator()
        + "Pull queries require a WHERE clause that:"
        + System.lineSeparator()
        + " - limits the query to a single ROWKEY, e.g. `SELECT * FROM X WHERE ROWKEY=Y;`."
        + additional
    );
  }

  private static Struct asKeyStruct(final Object rowKey, final PhysicalSchema physicalSchema) {
    final Struct key = new Struct(physicalSchema.keySchema().ksqlSchema());
    key.put(SchemaUtil.ROWKEY_NAME.name(), rowKey);
    return key;
  }
}
