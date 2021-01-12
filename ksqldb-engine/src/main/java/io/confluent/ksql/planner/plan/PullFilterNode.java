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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
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
import io.confluent.ksql.schema.ksql.Column.Namespace;
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
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

  private final boolean isWindowed;
  private final ExpressionMetadata compiledWhereClause;
  private final boolean addAdditionalColumnsToIntermediateSchema;
  private final LogicalSchema intermediateSchema;
  private final MetaStore metaStore;
  private final KsqlConfig ksqlConfig;
  private final LogicalSchema schema = getSource().getSchema();

  private Expression rewrittenPredicate;
  private Optional<WindowBounds> windowBounds;
  private List<GenericKey> keyValues;
  private Set<UnqualifiedColumnReferenceExp> keyColumns;
  private Set<UnqualifiedColumnReferenceExp> systemColumns;

  public PullFilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final boolean isWindowed
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    Objects.requireNonNull(predicate, "predicate");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.rewrittenPredicate = new StatementRewriteForMagicPseudoTimestamp().rewrite(predicate);
    this.isWindowed = isWindowed;

    // Basic validation of WHERE clause
    validateWhereClause();

    // Validation and extractions of window bounds
    windowBounds = isWindowed ? Optional.of(extractWindowBounds()) : Optional.empty();

    // Extraction of key and system columns
    extractKeysAndSystemCols();

    // Extraction of key values
    keyValues = extractKeyValues();

    // Compiling expression into byte code
    this.addAdditionalColumnsToIntermediateSchema = shouldAddAdditionalColumnsInSchema();
    this.intermediateSchema = PullLogicalPlanUtil.buildIntermediateSchema(
        source.getSchema(), addAdditionalColumnsToIntermediateSchema, isWindowed);
    compiledWhereClause = CodeGenRunner.compileExpression(
        rewrittenPredicate,
        "Predicate",
        intermediateSchema,
        ksqlConfig,
        metaStore
    );
  }

  public Expression getRewrittenPredicate() {
    return rewrittenPredicate;
  }

  @Override
  public LogicalSchema getSchema() {
    return getSource().getSchema();
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildCtx) {
    throw new UnsupportedOperationException();
  }

  public ExpressionMetadata getCompiledWhereClause() {
    return compiledWhereClause;
  }

  public boolean isWindowed() {
    return isWindowed;
  }

  public List<GenericKey> getKeyValues() {
    return keyValues;
  }

  public Optional<WindowBounds> getWindowBounds() {
    return windowBounds;
  }

  public boolean getAddAdditionalColumnsToIntermediateSchema() {
    return addAdditionalColumnsToIntermediateSchema;
  }

  public LogicalSchema getIntermediateSchema() {
    return intermediateSchema;
  }

  private void validateWhereClause() {
    final Validator validator = new Validator();
    validator.process(rewrittenPredicate, null);
    if (!validator.isKeyedQuery) {
      throw invalidWhereClauseException("WHERE clause missing key column", isWindowed);
    }

    if (!validator.seenKeys.isEmpty() && validator.seenKeys.cardinality() != schema.key().size()) {
      final List<ColumnName> seenKeyNames = validator.seenKeys
          .stream()
          .boxed()
          .map(i -> schema.key().get(i))
          .map(Column::name)
          .collect(Collectors.toList());
      throw invalidWhereClauseException(
          "Multi-column sources must specify every key in the WHERE clause. Specified: "
              + seenKeyNames + " Expected: " + schema.key(), isWindowed);
    }
  }

  private void extractKeysAndSystemCols() {
    keyColumns = new HashSet<>();
    systemColumns = new HashSet<>();
    new KeyAndSystemColsExtractor().process(rewrittenPredicate, null);
  }

  /**
   * The WHERE clause is currently limited to either having a single IN predicate
   * or equality conditions on the keys.
   * inKeys has the key values as specified in the IN predicate.
   * seenKeys is used to make sure that all columns of a multi-column
   * key are constrained via an equality condition.
   * keyContents has the key values for each columns of a key.
   * @return the constrains on the key values used to to do keyed lookup.
   */
  private List<GenericKey> extractKeyValues() {
    final KeyValueExtractor keyValueExtractor = new KeyValueExtractor();
    keyValueExtractor.process(rewrittenPredicate, null);
    if (!keyValueExtractor.inKeys.isEmpty()) {
      return keyValueExtractor.inKeys;
    }

    return ImmutableList.of(GenericKey.fromList(Arrays.asList(keyValueExtractor.keyContents)));
  }

  private WindowBounds extractWindowBounds() {
    final WindowBounds windowBounds = new WindowBounds();

    new WindowBoundsExtractor().process(rewrittenPredicate, windowBounds);
    return windowBounds;
  }

  /**
   * Validate the WHERE clause for pull queries.
   * 1. There must be exactly one equality condition per key
   * or one IN predicate that involves a key.
   * 2. An IN predicate can refer to a single key.
   * 3. The IN predicate cannot be combined with other conditions.
   * 4. Only AND is allowed.
   * 5. If there is a multi-key, conditions on all keys must be specified.
   * 6. The IN predicate cannot use multi-keys.
   */
  private final class Validator extends TraversalExpressionVisitor<Object> {
    private final BitSet seenKeys;
    private boolean containsINkeys;
    private boolean isKeyedQuery;

    Validator() {
      isKeyedQuery = false;
      seenKeys = new BitSet(schema.key().size());
      containsINkeys = false;
    }

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
      final UnqualifiedColumnReferenceExp column = getColumnRefSide(node);

      final ColumnName columnName = column.getColumnName();
      if (columnName.equals(SystemColumns.WINDOWSTART_NAME)
          || columnName.equals(SystemColumns.WINDOWEND_NAME)) {
        final Type type = node.getType();
        if (!VALID_WINDOW_BOUND_COMPARISONS.contains(type)) {
          throw invalidWhereClauseException(
              "Unsupported " + columnName + " bounds: " + type, true);
        }
        if (!isWindowed) {
          throw invalidWhereClauseException(
              "Cannot use WINDOWSTART/WINDOWEND on non-windowed source",
              false);
        }
        return null;
      } else {
        final Column col = schema.findColumn(columnName)
            .orElseThrow(() -> invalidWhereClauseException(
                "Bound on non-key column " + columnName, isWindowed));

        if (col.namespace() == Namespace.KEY) {
          if (node.getType() != Type.EQUAL) {
            throw invalidWhereClauseException(
                "Bound on key columns '" + getSource().getSchema().key()
                    + "' must currently be '='",
                isWindowed);
          }
          if (containsINkeys || seenKeys.get(col.index())) {
            throw invalidWhereClauseException(
                "An equality condition on the key column cannot be combined with other comparisons"
                    + " such as an IN predicate",
                isWindowed);
          }
          seenKeys.set(col.index());
          isKeyedQuery = true;
          return null;
        }

        throw invalidWhereClauseException(
            "WHERE clause on non-key column: " + columnName.text(),
            false
        );
      }
    }

    @Override
    public Void visitInPredicate(
        final InPredicate node,
        final Object context
    ) {
      if (schema.key().size() > 1) {
        throw invalidWhereClauseException(
            "Schemas with multiple KEY columns are not supported for IN predicates", false);
      }

      final UnqualifiedColumnReferenceExp column
          = (UnqualifiedColumnReferenceExp) node.getValue();
      final Optional<Column> col = schema.findColumn(column.getColumnName());
      if (col.isPresent() && col.get().namespace() == Namespace.KEY) {
        if (!seenKeys.isEmpty()) {
          throw invalidWhereClauseException(
              "The IN predicate cannot be combined with other comparisons on the key column",
              isWindowed);
        }
        containsINkeys = true;
        isKeyedQuery = true;
      } else {
        throw invalidWhereClauseException(
            "WHERE clause on unsupported column: " + column.getColumnName().text(),
            false
        );
      }
      return null;
    }
  }

  private UnqualifiedColumnReferenceExp getColumnRefSide(final ComparisonExpression comp) {
    return (UnqualifiedColumnReferenceExp)
        (comp.getRight() instanceof UnqualifiedColumnReferenceExp
            ? comp.getRight() : comp.getLeft());
  }

  private Expression getNonColumnRefSide(final ComparisonExpression comparison) {
    return comparison.getRight() instanceof UnqualifiedColumnReferenceExp
        ? comparison.getLeft()
        : comparison.getRight();
  }

  /**
   * Extracts the key and system columns that appear in the WHERE clause.
   */
  private final class KeyAndSystemColsExtractor extends TraversalExpressionVisitor<Object> {

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node, final Object context) {
      final Optional<Column> col = schema.findColumn(node.getColumnName());
      if (col.isPresent() && col.get().namespace() == Namespace.KEY) {
        keyColumns.add(node);
      } else if (SystemColumns.isSystemColumn(node.getColumnName())) {
        systemColumns.add(node);
      }
      return null;
    }
  }

  /**
   * Extracts the values for the keys that appear in the WHERE clause.
   * Necessary so that we can do key lookups when scanning the data stores.
   */
  private final class KeyValueExtractor extends TraversalExpressionVisitor<Object> {
    private final List<GenericKey> inKeys;
    private final BitSet seenKeys;
    private final Object[] keyContents;

    KeyValueExtractor() {
      inKeys = new ArrayList<>();
      keyContents = new Object[schema.key().size()];
      seenKeys = new BitSet(schema.key().size());
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node, final Object context) {
      final UnqualifiedColumnReferenceExp column = getColumnRefSide(node);
      final Expression other = getNonColumnRefSide(node);
      final ColumnName columnName = column.getColumnName();

      final Optional<Column> col = schema.findColumn(columnName);
      if (col.isPresent() && col.get().namespace() == Namespace.KEY) {
        final Object key = resolveKey(other, col.get(), metaStore, ksqlConfig, node);
        keyContents[col.get().index()] = key;
        seenKeys.set(col.get().index());
      }
      return null;
    }

    @Override
    public Void visitInPredicate(
        final InPredicate node, final Object context) {
      final UnqualifiedColumnReferenceExp column
          = (UnqualifiedColumnReferenceExp) node.getValue();
      final Optional<Column> col = schema.findColumn(column.getColumnName());
      if (col.isPresent() && col.get().namespace() == Namespace.KEY) {
        inKeys.addAll(node.getValueList()
            .getValues()
            .stream()
            .map(expression -> resolveKey(expression, col.get(), metaStore, ksqlConfig, node))
            .map(GenericKey::genericKey)
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
          .orElseThrow(() -> new KsqlException(
              "'" + obj + "' can not be converted "
                  + "to the type of the key column: "
                  + keyColumn.toString(
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
  private static final class WindowBoundsExtractor
      extends TraversalExpressionVisitor<WindowBounds> {

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

    private BoundType getRangeBoundType(final ComparisonExpression lowerComparison) {
      final boolean openBound = lowerComparison.getType() == Type.LESS_THAN
          || lowerComparison.getType() == Type.GREATER_THAN;

      return openBound
          ? BoundType.OPEN
          : BoundType.CLOSED;
    }
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
            + " - limits the query to keys only, e.g. `SELECT * FROM X WHERE <key-column>=Y;`."
            + System.lineSeparator()
            + " - specifies an equality condition that is a conjunction of equality expressions "
            + "that cover all keys."
            + additional
    );
  }

  public static final class WindowBounds {

    private WindowRange start;
    private WindowRange end;

    public WindowBounds(final WindowRange start, final WindowRange end) {
      this.start = Objects.requireNonNull(start, "startBounds");
      this.end = Objects.requireNonNull(end, "endBounds");
    }

    public WindowBounds() {
      this.start = new WindowRange();
      this.end = new WindowRange();
    }

    boolean setEquality(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.equal != null) {
          return false;
        }
        start.equal = range;
      } else {
        if (end.equal != null) {
          return false;
        }
        end.equal = range;
      }
      return true;
    }

    boolean setUpper(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.upper != null) {
          return false;
        }
        start.upper = range;
      } else {
        if (end.upper != null) {
          return false;
        }
        end.upper = range;
      }
      return true;
    }

    boolean setLower(
        final UnqualifiedColumnReferenceExp column,
        final Range<Instant> range
    ) {
      if (column.getColumnName().equals(SystemColumns.WINDOWSTART_NAME)) {
        if (start.lower != null) {
          return false;
        }
        start.lower = range;
      } else {
        if (end.lower != null) {
          return false;
        }
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

      WindowRange(
          final Range<Instant> equal,
          final Range<Instant> upper,
          final Range<Instant> lower
      ) {
        this.equal = equal;
        this.upper = upper;
        this.lower = lower;
      }

      WindowRange() {
      }

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
        if (lower == null && upper == null && equal == null) {
          return Range.all();
        }
        if (lower != null && upper != null) {
          return Range.range(
              lower.lowerEndpoint(), lower.lowerBoundType(),
              upper.upperEndpoint(), upper.upperBoundType()
          );
        }
        if (upper != null) {
          return upper;
        }
        if (lower != null) {
          return lower;
        }
        return equal;
      }

      @Override
      public boolean equals(final Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        final WindowRange that = (WindowRange) o;
        return Objects.equals(equal, that.equal)
            && Objects.equals(upper, that.upper)
            && Objects.equals(lower, that.lower);
      }

      @Override
      public int hashCode() {
        return Objects.hash(equal, upper, lower);
      }

      @Override
      public String toString() {
        return "WindowRange{"
            + "equal=" + equal
            + ", upper=" + upper
            + ", lower=" + lower
            + '}';
      }
    }
  }

  /**
   * Checks whether the intermediate schema should be extended with system and key columns.
   * @return true if the intermediate schema should be extended
   */
  private boolean shouldAddAdditionalColumnsInSchema() {

    final boolean hasSystemColumns = !systemColumns.isEmpty();

    final boolean hasKeyColumns = !keyColumns.isEmpty();

    return hasSystemColumns || hasKeyColumns;
  }
}
