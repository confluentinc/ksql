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

package io.confluent.ksql.planner.plan;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PullFilterNode extends SingleSourcePlanNode {

  private static final Set<Type> VALID_WINDOW_BOUND_COMPARISONS = ImmutableSet.of(
      Type.EQUAL,
      Type.GREATER_THAN,
      Type.GREATER_THAN_OR_EQUAL,
      Type.LESS_THAN,
      Type.LESS_THAN_OR_EQUAL
  );

  private final static String KEY_COL = "keys";
  private final static String SYSTEM_COL = "system";

  private final Expression predicate;
  private final boolean isWindowed;
  private final RewrittenAnalysis analysis;
  private final ExpressionMetadata compiledWhereClause;
  private final boolean addAdditionalColumnsToIntermediateSchema;
  private final LogicalSchema intermediateSchema;
  private final MetaStore metaStore;
  private final KsqlConfig ksqlConfig;

  private boolean isKeyedQuery = false;
  private Map<String, Set<UnqualifiedColumnReferenceExp>> keysAndSystemCols;
  private WindowBounds windowBounds;

  public PullFilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate,
      final RewrittenAnalysis analysis,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    this.predicate = Objects.requireNonNull(predicate, "predicate");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();

    final Validator validator = new Validator();
    validator.process(predicate, null);
    if (!isKeyedQuery) {
      throw invalidWhereClauseException("WHERE clause missing key column", isWindowed);
    }
    windowBounds = extractWindowBounds();
    keysAndSystemCols = extractKeysAndSystemCols();
    this.addAdditionalColumnsToIntermediateSchema = shouldAddAdditionalColumnsInSchema();
    this.intermediateSchema = buildIntermediateSchema();
    compiledWhereClause = CodeGenRunner.compileExpression(
        predicate,
        "Predicate",
        intermediateSchema,
        ksqlConfig,
        metaStore
    );

  }

  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public LogicalSchema getSchema() {
    return getSource().getSchema();
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    throw new UnsupportedOperationException();
  }

  public ExpressionMetadata getCompiledWhereClause() {
    return compiledWhereClause;
  }

  public boolean isKeyedQuery() {
    return isKeyedQuery;
  }

  public boolean isWindowed() {
    return isWindowed;
  }

  public List<Object> getKeyValues() {
    final List<Object> keys = new ArrayList<>();

    new KeyValueExtractor().process(predicate, keys);
    return keys;
  }

  public Set<UnqualifiedColumnReferenceExp> getKeyColumns() {
    return keysAndSystemCols.get(KEY_COL);
  }

  public WindowBounds getWindowBounds() {
    return windowBounds;
  }

  public boolean getAddAdditionalColumnsToIntermediateSchema() {
    return addAdditionalColumnsToIntermediateSchema;
  }

  public LogicalSchema getIntermediateSchema() {
    return intermediateSchema;
  }

  private Map<String, Set<UnqualifiedColumnReferenceExp>> extractKeysAndSystemCols() {
    final Map<String, Set<UnqualifiedColumnReferenceExp>> cols = new HashMap<>();

    new KeyAndSystemColsExtractor().process(predicate, cols);
    return cols;
  }

  private WindowBounds extractWindowBounds() {
    final WindowBounds windowBounds = new WindowBounds();

    new WindowBoundsExtractor().process(predicate, windowBounds);
    return windowBounds;
  }

  /**
   * Validate the WHERE clause for pull queries.
   * 1. There must be exactly one equality condition or one IN predicate that involves a key.
   * 2. An IN predicate can refer to a single key.
   * 3. The IN predicate cannot be combined with other conditions.
   * 4. Only AND is allowed.
   */
  private final class Validator extends TraversalExpressionVisitor<Object> {

    @Override
    public Void process(final Expression node, final Object context) {
      if (!(node instanceof  LogicalBinaryExpression)
          && !(node instanceof  ComparisonExpression)
          && !(node instanceof  InPredicate)) {
        throw invalidWhereClauseException("Unsupported expression in WHERE clause: " + node, false);
      }
      super.process(node, context);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context
    ) {
      if (node.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + node, false);
      }
      process(node.getLeft(), context);
      process(node.getRight(), context);
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node,
        final Object context
    ) {
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();

      final UnqualifiedColumnReferenceExp column;
      if (node.getRight() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getRight();
      } else if (node.getLeft() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getLeft();
      } else {
        return null;
      }
      final ColumnName columnName = column.getColumnName();

      if (columnName.equals(keyColumn)) {
        if (node.getType() != Type.EQUAL) {
          throw invalidWhereClauseException(
              "Bound on '" + keyColumn.text() + "' must currently be '='", isWindowed);
        }
        if (isKeyedQuery) {
          throw invalidWhereClauseException(
              "An equality condition on the key column cannot be combined with other comparisons"
                  + " such as an IN predicate",
              isWindowed);
        }
        isKeyedQuery = true;
      } else if (columnName.equals(SystemColumns.WINDOWSTART_NAME)
          || columnName.equals(SystemColumns.WINDOWEND_NAME)) {
        final Type type = node.getType();
        if (!VALID_WINDOW_BOUND_COMPARISONS.contains(type)) {
          throw invalidWhereClauseException(
              "Unsupported " + columnName + " bounds: " + type, true);
        }

        //  check if bounds on windowed column is among the allowed ones
        //  check if equality bound is combined with lesser/greater and throw
        //  check for duplicate bounds
        if (!isWindowed) {
          throw invalidWhereClauseException(
              "Cannot use WINDOWSTART/WINDOWEND on non-windowed source",
              false);
        }
      } else {
        throw invalidWhereClauseException(
            "WHERE clause on unsupported column: " + columnName.text(),
            false
        );
      }
      return null;
    }

    @Override
    public Void visitInPredicate(
        final InPredicate node,
        final Object context
    ) {
      final UnqualifiedColumnReferenceExp column
          = (UnqualifiedColumnReferenceExp) node.getValue();
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();
      if (column.getColumnName().equals(keyColumn)) {
        if (isKeyedQuery) {
          throw invalidWhereClauseException(
              "The IN predicate cannot be combined with other comparisons on the key column",
              isWindowed);
        }
        //  Check if column is not key and throw
        isKeyedQuery = true;
      }
      return null;
    }
  }

  /**
   * Extracts the key columns that appear in the WHERE clause.
   */
  private final class KeyAndSystemColsExtractor
      extends TraversalExpressionVisitor<Map<String, Set<UnqualifiedColumnReferenceExp>> > {

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Map<String, Set<UnqualifiedColumnReferenceExp>> cols
    ) {
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();
      if (node.getColumnName().equals(keyColumn)) {
        cols.putIfAbsent(KEY_COL, new HashSet<>());
        cols.get(KEY_COL).add(node);
      } else if (SystemColumns.isSystemColumn(node.getColumnName())) {
        cols.putIfAbsent(SYSTEM_COL, new HashSet<>());
        cols.get(SYSTEM_COL).add(node);
      }
      return null;
    }
  }

  /**
   * Extracts the values for the keys that appear in the WHERE clause.
   * Necessary so that we can do key lookups when scanning the data stores.
   */
  private final class KeyValueExtractor extends TraversalExpressionVisitor<List<Object>> {

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node,
        final List<Object> keys
    ) {
      final Column keyColumn = Iterables.getOnlyElement(getSource().getSchema().key());
      final ColumnName keyColumnName = keyColumn.name();

      final UnqualifiedColumnReferenceExp column;
      final Expression other;
      if (node.getRight() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getRight();
        other = node.getLeft();
      } else if (node.getLeft() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getLeft();
        other = node.getRight();
      } else {
        return null;
      }
      final ColumnName columnName = column.getColumnName();

      if (columnName.equals(keyColumnName)) {
        keys.add(resolveKey(other, keyColumn, metaStore, ksqlConfig, node));
      }
      return null;
    }

    @Override
    public Void visitInPredicate(
        final InPredicate node,
        final List<Object> keys
    ) {
      final InPredicate inPredicate = (InPredicate) predicate;
      final UnqualifiedColumnReferenceExp column
          = (UnqualifiedColumnReferenceExp) inPredicate.getValue();
      final Column keyColumn = Iterables.getOnlyElement(getSource().getSchema().key());
      final ColumnName keyColumnName = keyColumn.name();
      if (column.getColumnName().equals(keyColumnName)) {
        keys.addAll(inPredicate.getValueList()
            .getValues()
            .stream()
            .map(expression -> resolveKey(expression, keyColumn, metaStore, ksqlConfig, inPredicate))
            .collect(Collectors.toList()));
      }
      return null;
    }

    private Object resolveKey(
        final Expression exp,
        final Column keyColumn,
        final MetaStore metaStore,
        final KsqlConfig config,
        final Expression errorMessageHint
    ) {
      final Object obj;
      if (exp instanceof NullLiteral) {
        obj = null;
      } else if (exp instanceof Literal) {
        // skip the GenericExpressionResolver because this is
        // a critical code path executed once-per-query
        obj = ((Literal) exp).getValue();
      } else {
        obj = new GenericExpressionResolver(
            keyColumn.type(),
            keyColumn.name(),
            metaStore,
            config,
            "pull query"
        ).resolve(exp);
      }

      if (obj == null) {
        throw new KsqlException("Primary key columns can not be NULL: " + errorMessageHint);
      }

      return DefaultSqlValueCoercer.STRICT.coerce(obj, keyColumn.type())
          .orElseThrow(() -> new KsqlException("'" + obj + "' can not be converted "
                                                   + "to the type of the key column: " + keyColumn.toString(
              FormatOptions.noEscape())))
          .orElse(null);
    }
  }


  /**
   * Extracts the upper and lower bounds on windowstart/windowend columns.
   * Performs the following validations on the window bounds:
   * 1. An equality bound cannot be combined with other bounds.
   * 2. No duplicate bounds are allowed, such as multiple greater than bounds.
   */
  private final class WindowBoundsExtractor extends TraversalExpressionVisitor<WindowBounds> {

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node,
        final WindowBounds windowBounds
    ) {
      final UnqualifiedColumnReferenceExp column;
      if (node.getRight() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getRight();
      } else if (node.getLeft() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) node.getLeft();
      } else {
        return null;
      }

      if (!column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)
          && !column.getColumnName().equals(SystemColumns.WINDOWEND_NAME)) {
        return null;
      }
      boolean result = false;
      if (node.getType().equals(Type.EQUAL)) {
        final Range<Instant> instant = Range.singleton(asInstant(getNonColumnRefSide(node)));
        result = windowBounds.setEquality(column, instant);
      }
      final Type type = getSimplifiedBoundType(node);

      if (type.equals(Type.LESS_THAN)) {
        final Instant upper = asInstant(getNonColumnRefSide(node));
        final BoundType upperType = getRangeBoundType(node);
        result = windowBounds.setUpper(column, Range.upTo(upper, upperType));
      } else if (type.equals(Type.GREATER_THAN)) {
        final Instant lower = asInstant(getNonColumnRefSide(node));
        final BoundType lowerType = getRangeBoundType(node);
        result = windowBounds.setLower(column, Range.downTo(lower, lowerType));
      }
      validateEqualityBound(windowBounds, node, column);
      if (!result) {
        throw invalidWhereClauseException(
            "Duplicate " + column.getColumnName() + " bounds on: " + type, true);
      }
      return null;
    }

    private void validateEqualityBound(
        final WindowBounds bound,
        final ComparisonExpression expression,
        final UnqualifiedColumnReferenceExp column
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (bound.getStart().getEqual() != null
            && (bound.getStart().getUpper() != null || bound.getStart().getLower() != null)) {
          throw invalidWhereClauseException(
              "`" + expression + "` cannot be combined with other " + column.getColumnName()
                  + " bounds",
              true
          );
        }
      } else {
        if (bound.getEnd().getEqual() != null
            && (bound.getEnd().getUpper() != null || bound.getEnd().getLower() != null)) {
          throw invalidWhereClauseException(
              "`" + expression + "` cannot be combined with other " + column.getColumnName()
                  + " bounds",
              true
          );

        }
      }
    }

    private Type getSimplifiedBoundType(final ComparisonExpression comparison) {
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

    private Expression getNonColumnRefSide(final ComparisonExpression comparison) {
      return comparison.getRight() instanceof UnqualifiedColumnReferenceExp
          ? comparison.getLeft()
          : comparison.getRight();
    }

    private Instant asInstant(final Expression other) {
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

    private Range<Instant> extractWindowBound(
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

    private BoundType getRangeBoundType(final ComparisonExpression lowerComparison) {
      final boolean openBound = lowerComparison.getType() == Type.LESS_THAN
          || lowerComparison.getType() == Type.GREATER_THAN;

      return openBound
          ? BoundType.OPEN
          : BoundType.CLOSED;
    }
  }

  private KsqlException invalidWhereClauseException(
      final String msg,
      final boolean windowed
  ) {
    final String additional = !windowed
        ? ""
        : System.lineSeparator()
            + " - (optionally) limits the time bounds of the windowed table."
            + System.lineSeparator()
            + "\t Bounds on " + SystemColumns.windowBoundsColumnNames() + " are supported"
            + System.lineSeparator()
            + "\t Supported operators are " + VALID_WINDOW_BOUND_COMPARISONS;

    return new KsqlException(
        msg
            + ". "
            + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
            + System.lineSeparator()
            + "Pull queries require a WHERE clause that:"
            + System.lineSeparator()
            + " - limits the query to a single key, e.g. `SELECT * FROM X WHERE <key-column>=Y;`."
            + additional
    );
  }

  public static final class WindowBounds {

    private WindowRange start;
    private WindowRange end;

    public WindowBounds(final WindowRange start, final WindowRange end) {
      this.start = start;
      this.end = end;
    }

    public WindowBounds() {
      this.start = new WindowRange();
      this.end = new WindowRange();
    }

    public boolean setEquality(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.equal != null)
          return false;
        start.equal = range;
      } else {
        if (end.equal != null)
          return false;
        end.equal = range;
      }
      return true;
    }

    public boolean setUpper(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.upper != null)
          return false;
        start.upper = range;
      } else {
        if (end.upper != null)
          return false;
        end.upper = range;
      }
      return true;
    }

    public boolean setLower(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.lower != null)
          return false;
        start.lower = range;
      } else {
        if (end.lower != null)
          return false;
        end.lower = range;
      }
      return true;
    }

    public WindowRange getStart() {
      return start;
    }

    public WindowRange getEnd() {
      return end;
    }

    public Range<Instant> getMergedStart() {
      return start.getMergedRange();
    }

    public Range<Instant> getMergedEnd() {
      return end.getMergedRange();
    }


    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final WindowBounds that = (WindowBounds) o;
      return Objects.equals(start, that.start)
          && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, end);
    }

    @Override
    public String toString() {
      return "WindowBounds{"
          + "start=" + start
          + ", end=" + end
          + '}';
    }

    static final class WindowRange {
      private Range<Instant> equal;
      private Range<Instant> upper;
      private Range<Instant> lower;

      public Range<Instant> getEqual() {
        return equal;
      }

      public Range<Instant> getUpper() {
        return upper;
      }

      public Range<Instant> getLower() {
        return lower;
      }

      Range<Instant> getMergedRange() {
        if (lower != null && upper != null) {
          return Range.range(lower.lowerEndpoint(), lower.lowerBoundType(),
                             upper.upperEndpoint(), upper.upperBoundType());
        }
        if (upper != null) {
          return upper;
        }
        if (lower != null) {
          return lower;
        }
        return equal;
      }
    }
  }

  /**
   * Builds the schema used for codegen to compile expressions into bytecode. The input schema may
   * need to be extended with system columns if they are part of the projection.
   * @return the intermediate schema
   */
  private LogicalSchema buildIntermediateSchema() {
    final LogicalSchema parentSchema = getSource().getSchema();

    if (!addAdditionalColumnsToIntermediateSchema) {
      return parentSchema;
    } else {
      return parentSchema
          .withPseudoAndKeyColsInValue(isWindowed);
    }
  }

  /**
   * Checks whether the intermediate schema should be extended with system and key columns.
   * @return true if the intermediate schema should be extended
   */
  private boolean shouldAddAdditionalColumnsInSchema() {

    final boolean hasSystemColumns = keysAndSystemCols.containsKey(SYSTEM_COL);

    final boolean hasKeyColumns = keysAndSystemCols.containsKey(KEY_COL);

    return hasSystemColumns || hasKeyColumns;
  }
}
