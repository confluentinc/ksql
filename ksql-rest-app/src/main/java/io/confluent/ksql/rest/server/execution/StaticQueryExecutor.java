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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.QueryAnalyzer;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.SelectValueMapper;
import io.confluent.ksql.execution.streams.SelectValueMapperFactory;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.materialization.Locator;
import io.confluent.ksql.materialization.Locator.KsqlNode;
import io.confluent.ksql.materialization.Materialization;
import io.confluent.ksql.materialization.MaterializationTimeOutException;
import io.confluent.ksql.materialization.TableRow;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.entity.TableRowsEntityFactory;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
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
import io.confluent.ksql.util.timestamp.StringToTimestampParser;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class StaticQueryExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Duration OWNERSHIP_TIMEOUT = Duration.ofSeconds(30);
  private static final Set<Type> VALID_WINDOW_BOUNDS_TYPES = ImmutableSet.of(
      Type.EQUAL, Type.GREATER_THAN_OR_EQUAL, Type.LESS_THAN
  );
  private static final String VALID_WINDOW_BOUNDS_TYPES_STRING =
      VALID_WINDOW_BOUNDS_TYPES.toString();

  private StaticQueryExecutor() {
  }

  public static void validate(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final Query queryStmt = statement.getStatement();

    if (!queryStmt.isStatic()) {
      throw new KsqlRestException(Errors.queryEndpoint(statement.getStatementText()));
    }

    try {
      final Analysis analysis = analyze(statement, executionContext);

      final PersistentQueryMetadata query = findMaterializingQuery(executionContext, analysis);

      extractWhereInfo(analysis, query);
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(),
          statement.getStatementText(),
          e
      );
    }
  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    try {
      final Analysis analysis = analyze(statement, executionContext);

      final PersistentQueryMetadata query = findMaterializingQuery(executionContext, analysis);

      final WhereInfo whereInfo = extractWhereInfo(analysis, query);

      final QueryContext.Stacker contextStacker = new Stacker(new QueryId("static-query"));

      final Materialization mat = query
          .getMaterialization(contextStacker)
          .orElseThrow(() -> notMaterializedException(getSourceName(analysis)));

      final Struct rowKey = asKeyStruct(whereInfo.rowkey, query.getPhysicalSchema());

      final KsqlNode owner = getOwner(rowKey, mat);
      if (!owner.isLocal()) {
        return Optional.of(proxyTo(owner, statement));
      }

      Result result;
      if (whereInfo.windowBounds.isPresent()) {
        final WindowBounds windowBounds = whereInfo.windowBounds.get();

        final List<? extends TableRow> rows = mat.windowed()
            .get(rowKey, windowBounds.lower, windowBounds.upper);

        result = new Result(mat.schema(), rows);
      } else {
        final List<? extends TableRow> rows = mat
            .nonWindowed().get(rowKey)
            .map(ImmutableList::of)
            .orElse(ImmutableList.of());

        result = new Result(mat.schema(), rows);
      }

      result = handleSelects(result, statement, executionContext, analysis);

      final TableRowsEntity entity = new TableRowsEntity(
          statement.getStatementText(),
          TableRowsEntityFactory.buildSchema(result.schema, mat.windowType()),
          TableRowsEntityFactory.createRows(result.rows)
      );

      return Optional.of(entity);
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(),
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

  private static final class WindowBounds {

    private final Instant lower;
    private final Instant upper;

    private WindowBounds(final Instant lower, final Instant upper) {
      this.lower = requireNonNull(lower, "lower");
      this.upper = requireNonNull(upper, "upper'");

      if (lower.isAfter(upper)) {
        throw new KsqlException("Lower window bound must be less than upper bound");
      }
    }
  }

  private static final class WhereInfo {

    private final Object rowkey;
    private final Optional<WindowBounds> windowBounds;

    private WhereInfo(
        final Object rowkey,
        final Optional<WindowBounds> windowBounds
    ) {
      this.rowkey = rowkey;
      this.windowBounds = windowBounds;
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

    final List<ComparisonExpression> windowBoundsComparison =
        comparisons.get(ComparisonTarget.WINDOWSTART);

    if (windowBoundsComparison == null) {
      throw invalidWhereClauseException(
          "WHERE clause missing " + ComparisonTarget.WINDOWSTART,
          true
      );
    }

    final WindowBounds windowBounds = extractWhereClauseWindowBounds(windowBoundsComparison);

    return new WhereInfo(rowKey, Optional.of(windowBounds));
  }

  private static Object extractRowKeyWhereClause(
      final List<ComparisonExpression> comparisons,
      final boolean windowed
  ) {
    if (comparisons.size() != 1) {
      throw invalidWhereClauseException("Multiple bounds on ROWKEY", windowed);
    }

    final ComparisonExpression comparison = comparisons.get(0);

    final Expression other = comparison.getRight() instanceof QualifiedNameReference
        ? comparison.getLeft()
        : comparison.getRight();

    if (!(other instanceof StringLiteral)) {
      throw invalidWhereClauseException("ROWKEY must be comparsed to STRING literal.", false);
    }

    if (comparison.getType() != Type.EQUAL) {
      throw invalidWhereClauseException("ROWKEY bound must currently be '='.", false);
    }

    final Literal right = (Literal) other;
    return right.getValue();
  }

  private static WindowBounds extractWhereClauseWindowBounds(
      final List<ComparisonExpression> comparisons
  ) {
    final Map<Type, List<ComparisonExpression>> byType = comparisons.stream()
        .collect(Collectors.groupingBy(StaticQueryExecutor::getBoundType));

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

      final Instant windowStart = extractWindowBound(equals);
      return new WindowBounds(windowStart, windowStart);
    }

    final ComparisonExpression upper = singles.get(Type.LESS_THAN);
    if (upper == null) {
      throw invalidWhereClauseException(
          "Missing upper bound on " + ComparisonTarget.WINDOWSTART,
          true
      );
    }

    final ComparisonExpression lower = singles.get(Type.GREATER_THAN_OR_EQUAL);
    if (lower == null) {
      throw invalidWhereClauseException(
          "Missing lower bound on " + ComparisonTarget.WINDOWSTART,
          true
      );
    }

    final Instant upperBound = extractWindowBound(upper);
    final Instant lowerBound = extractWindowBound(lower);
    return new WindowBounds(lowerBound, upperBound);
  }

  private static Type getBoundType(final ComparisonExpression comparison) {
    final Type type = comparison.getType();
    final boolean inverted = comparison.getRight() instanceof QualifiedNameReference;

    switch (type) {
      case LESS_THAN:
        return inverted ? Type.GREATER_THAN : type;
      case LESS_THAN_OR_EQUAL:
        return inverted ? Type.GREATER_THAN_OR_EQUAL : type;
      case GREATER_THAN:
        return inverted ? Type.LESS_THAN : type;
      case GREATER_THAN_OR_EQUAL:
        return inverted ? Type.LESS_THAN_OR_EQUAL : type;
      default:
        return type;
    }
  }

  private static Instant extractWindowBound(final ComparisonExpression comparison) {
    final Expression other = comparison.getRight() instanceof QualifiedNameReference
        ? comparison.getLeft()
        : comparison.getRight();

    final Instant bound = asInstant(other);

    switch (getBoundType(comparison)) {
      case LESS_THAN:
        return bound.minusMillis(1);
      case GREATER_THAN:
        return bound.plusMillis(1);
      default:
        return bound;
    }
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
        final long timestamp = new StringToTimestampParser(KsqlConstants.DATE_TIME_PATTERN)
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
    final QualifiedNameReference column;
    if (comparison.getRight() instanceof QualifiedNameReference) {
      column = (QualifiedNameReference) comparison.getRight();
    } else if (comparison.getLeft() instanceof QualifiedNameReference) {
      column = (QualifiedNameReference) comparison.getLeft();
    } else {
      throw invalidWhereClauseException("Invalid WHERE clause: " + comparison, false);
    }

    final String fieldName = column.getName().name();

    try {
      return ComparisonTarget.valueOf(fieldName.toUpperCase());
    } catch (final Exception e) {
      throw invalidWhereClauseException("WHERE clause on unsupported field: " + fieldName, false);
    }
  }

  private static boolean isSelectStar(final List<SelectItem> selects) {
    return selects.size() == 1 && selects.get(0) instanceof AllColumns;
  }

  private static Result handleSelects(
      final Result input,
      final ConfiguredStatement<Query> statement,
      final KsqlExecutionContext executionContext,
      final Analysis analysis
  ) {
    final List<SelectItem> selectItems = statement.getStatement().getSelect().getSelectItems();
    if (input.rows.isEmpty() || isSelectStar(selectItems)) {
      return input;
    }

    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    schemaBuilder.keyColumns(input.schema.key());

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        input.schema,
        executionContext.getMetaStore()
    );

    for (int idx = 0; idx < analysis.getSelectExpressions().size(); idx++) {
      final SelectExpression select = analysis.getSelectExpressions().get(idx);
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());
      schemaBuilder.valueColumn(select.getName(), type);
    }

    final LogicalSchema schema = schemaBuilder.build();

    final String sourceName = getSourceName(analysis);

    final KsqlConfig ksqlConfig = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    final SelectValueMapper mapper = SelectValueMapperFactory.create(
        analysis.getSelectExpressions(),
        input.schema.withAlias(sourceName),
        ksqlConfig,
        executionContext.getMetaStore(),
        NoopProcessingLogContext.INSTANCE.getLoggerFactory().getLogger("any")
    );

    final ImmutableList.Builder<TableRow> output = ImmutableList.builder();
    input.rows.forEach(r -> {
      final GenericRow mapped = mapper.apply(r.value());
      final TableRow tableRow = r.withValue(mapped, schema);
      output.add(tableRow);
    });

    return new Result(schema, output.build());
  }

  private static PersistentQueryMetadata findMaterializingQuery(
      final KsqlExecutionContext executionContext,
      final Analysis analysis
  ) {
    final MetaStore metaStore = executionContext.getMetaStore();

    final String sourceName = getSourceName(analysis);

    final Set<String> queries = metaStore.getQueriesWithSink(sourceName);
    if (queries.isEmpty()) {
      throw notMaterializedException(sourceName);
    }
    if (queries.size() > 1) {
      throw new KsqlException("Multiple queries currently materialize '" + sourceName + "'."
          + " KSQL currently only supports static queries when the table has only been"
          + " materialized once.");
    }

    final QueryId queryId = new QueryId(Iterables.get(queries, 0));

    return executionContext.getPersistentQuery(queryId)
        .orElseThrow(() -> new KsqlException("Materializing query has been stopped"));
  }

  private static String getSourceName(final Analysis analysis) {
    final DataSource<?> source = analysis.getFromDataSources().get(0).getDataSource();
    return source.getName();
  }

  private static KsqlNode getOwner(final Struct rowKey, final Materialization mat) {
    final Locator locator = mat.locator();

    final long threshold = System.currentTimeMillis() + OWNERSHIP_TIMEOUT.toMillis();
    while (System.currentTimeMillis() < threshold) {
      final Optional<KsqlNode> owner = locator.locate(rowKey);
      if (owner.isPresent()) {
        return owner.get();
      }
    }

    throw new MaterializationTimeOutException(
        "The owner of the key could not be determined within the configured timeout"
    );
  }

  private static KsqlEntity proxyTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement
  ) {
    try (KsqlRestClient client = new KsqlRestClient(owner.location().toString())) {

      final RestResponse<KsqlEntityList> response = client
          .makeKsqlRequest(statement.getStatementText());

      if (response.isErroneous()) {
        throw new KsqlServerException("Proxy attempt failed: " + response.getErrorMessage());
      }

      final KsqlEntityList entities = response.getResponse();
      if (entities.size() != 1) {
        throw new RuntimeException("Boom - expected 1 entity, got: " + entities.size());
      }

      return entities.get(0);
    }
  }

  private static KsqlException notMaterializedException(final String sourceTable) {
    return new KsqlException(
        "Table '" + sourceTable + "' is not materialized."
            + " KSQL currently only supports static queries on materialized aggregate tables."
            + " i.e. those created by a"
            + " 'CREATE TABLE AS SELECT <fields> FROM <sources> GROUP BY <key>' style statement.");
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
            + "    + a range, e.g. `WHERE a < WINDOWSTART AND WINDOWSTART < b"
            + System.lineSeparator()
            + "WINDOWSTART currently supports operators: " + VALID_WINDOW_BOUNDS_TYPES_STRING
            + System.lineSeparator()
            + "WINDOWSTART currently comparison with epoch milliseconds "
            + "or a datetime string in the form: " + KsqlConstants.DATE_TIME_PATTERN;

    return new KsqlException(msg
        + System.lineSeparator()
        + "Static queries currently require a WHERE clause that:"
        + System.lineSeparator()
        + " - limits the query to a single ROWKEY, e.g. `SELECT * FROM X WHERE ROWKEY=Y;`."
        + additional
    );
  }

  private static Struct asKeyStruct(final Object rowKey, final PhysicalSchema physicalSchema) {
    final Struct key = new Struct(physicalSchema.keySchema().ksqlSchema());
    key.put(SchemaUtil.ROWKEY_NAME, rowKey);
    return key;
  }
}
