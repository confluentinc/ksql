/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.physical.pull.operators;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
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
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extracts and validates the conditions in the WHERE clause of a pull query.
 * The assumptions are that the conditions must be either a single equality on a key or the IN
 * predicate on a list of keys. If the table is windowed, the WHERE clause can have addtionally
 * conditions on the window bounds, windowstart and windowend.
 */
public final class WhereInfo {

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


  private final List<Object> keysBound;
  private final Optional<WindowBounds> windowBounds;

  private WhereInfo(
      final List<Object> keysBound,
      final Optional<WindowBounds> windowBounds
  ) {
    this.keysBound = keysBound;
    this.windowBounds = Objects.requireNonNull(windowBounds);
  }

  public static WhereInfo extractWhereInfo(
      final Expression where,
      final LogicalSchema schema,
      final boolean windowed,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    if (schema.key().size() != 1) {
      throw invalidWhereClauseException("Schemas with multiple "
          + "KEY columns are not supported", windowed);
    }

    final KeyAndWindowBounds keyAndWindowBounds = extractComparisons(where, schema);
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
          extractKeyWhereClause(
              keyComparison,
              windowed,
              schema,
              metaStore,
              config)
      );
    } else {
      keys = extractKeysFromInPredicate(
          inPredicate,
          schema,
          metaStore,
          config
      );
    }

    if (!windowed) {
      if (keyAndWindowBounds.getWindowStartExpression().size() > 0
          || keyAndWindowBounds.getWindowEndExpression().size() > 0) {
        throw invalidWhereClauseException(
            "Cannot use WINDOWSTART/WINDOWEND on non-windowed source",
            false);
      }

      return new WhereInfo(keys, Optional.empty());
    }

    final WindowBounds windowBounds =
        extractWhereClauseWindowBounds(keyAndWindowBounds);

    return new WhereInfo(keys, Optional.of(windowBounds));
  }

  public List<Object> getKeysBound() {
    return keysBound;
  }

  public Optional<WindowBounds> getWindowBounds() {
    return windowBounds;
  }

  public static KsqlException invalidWhereClauseException(
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

  public boolean isWindowed() {
    return getWindowBounds().isPresent();
  }

  private static List<Object> extractKeysFromInPredicate(
      final List<InPredicate> inPredicates,
      final LogicalSchema schema,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final InPredicate inPredicate = Iterables.getLast(inPredicates);
    final Column keyColumn = Iterables.getOnlyElement(schema.key());
    return inPredicate.getValueList()
        .getValues()
        .stream()
        .map(expression -> resolveKey(expression, keyColumn, metaStore, config, inPredicate))
        .collect(Collectors.toList());
  }

  private static Object extractKeyWhereClause(
      final List<ComparisonExpression> comparisons,
      final boolean windowed,
      final LogicalSchema schema,
      final MetaStore metaStore,
      final KsqlConfig config
  ) {
    final ComparisonExpression comparison = Iterables.getLast(comparisons);
    if (comparison.getType() != Type.EQUAL) {
      final ColumnName keyColumn = Iterables.getOnlyElement(schema.key()).name();
      throw invalidWhereClauseException("Bound on '" + keyColumn.text()
          + "' must currently be '='", windowed);
    }

    final Expression other = getNonColumnRefSide(comparison);
    final Column keyColumn = schema.key().get(0);
    return resolveKey(other, keyColumn, metaStore, config, comparison);
  }


  private static Object resolveKey(
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
            + "to the type of the key column: " + keyColumn.toString(FormatOptions.noEscape())))
        .orElse(null);
  }

  private enum ComparisonTarget {
    KEYCOL,
    WINDOWSTART,
    WINDOWEND
  }

  private static Range<Instant> extractWhereClauseWindowBounds(
      final ComparisonTarget windowType,
      final List<ComparisonExpression> comparisons
  ) {
    if (comparisons.isEmpty()) {
      return Range.all();
    }

    final Map<Type, List<ComparisonExpression>> byType = comparisons.stream()
        .collect(Collectors.groupingBy(WhereInfo::getSimplifiedBoundType));

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

  private static KeyAndWindowBounds extractComparisons(
      final Expression exp,
      final LogicalSchema schema
  ) {
    if (exp instanceof ComparisonExpression) {
      final ComparisonExpression comparison = (ComparisonExpression) exp;
      return extractWhereClauseTarget(comparison, schema);
    }

    if (exp instanceof InPredicate) {
      final InPredicate inPredicate = (InPredicate) exp;
      return extractWhereClauseTarget(inPredicate, schema);
    }

    if (exp instanceof LogicalBinaryExpression) {
      final LogicalBinaryExpression binary = (LogicalBinaryExpression) exp;
      if (binary.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + exp, false);
      }

      final KeyAndWindowBounds left = extractComparisons(binary.getLeft(), schema);
      final KeyAndWindowBounds right = extractComparisons(binary.getRight(), schema);
      return left.merge(right);
    }

    throw invalidWhereClauseException("Unsupported expression: " + exp, false);
  }

  private static KeyAndWindowBounds extractWhereClauseTarget(
      final ComparisonExpression comparison,
      final LogicalSchema schema
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

    final ColumnName keyColumn = Iterables.getOnlyElement(schema.key()).name();
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
      final LogicalSchema schema
  ) {
    final UnqualifiedColumnReferenceExp column
        = (UnqualifiedColumnReferenceExp) inPredicate.getValue();
    final ColumnName keyColumn = Iterables.getOnlyElement(schema.key()).name();
    if (column.getColumnName().equals(keyColumn)) {
      return new KeyAndWindowBounds().addInPredicate(inPredicate);
    }

    throw invalidWhereClauseException(
        "IN expression on unsupported column: " + column.getColumnName().text(),
        false
    );
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

  private static BoundType getRangeBoundType(final ComparisonExpression lowerComparison) {
    final boolean openBound = lowerComparison.getType() == Type.LESS_THAN
        || lowerComparison.getType() == Type.GREATER_THAN;

    return openBound
        ? BoundType.OPEN
        : BoundType.CLOSED;
  }

  private static class KeyAndWindowBounds {
    private final List<ComparisonExpression> keyColExpression = new ArrayList<>();
    private final List<ComparisonExpression> windowStartExpression = new ArrayList<>();
    private final List<ComparisonExpression> windowEndExpression = new ArrayList<>();
    private final List<InPredicate> inPredicate = new ArrayList<>();

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

  public static final class WindowBounds {

    private final Range<Instant> start;
    private final Range<Instant> end;

    @VisibleForTesting
    WindowBounds(
        final Range<Instant> start,
        final Range<Instant> end
    ) {
      this.start = Objects.requireNonNull(start, "startBounds");
      this.end = Objects.requireNonNull(end, "endBounds");
    }

    public Range<Instant> getStart() {
      return start;
    }

    public Range<Instant> getEnd() {
      return end;
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


}
