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

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.interpreter.InterpretedExpressionFactory;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.planner.plan.KeyConstraint.ConstraintOperator;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFilterNode extends SingleSourcePlanNode {
  private static final Logger LOG = LoggerFactory.getLogger(QueryFilterNode.class);

  private static final Set<Type> VALID_WINDOW_BOUND_COMPARISONS = ImmutableSet.of(
      Type.EQUAL,
      Type.GREATER_THAN,
      Type.GREATER_THAN_OR_EQUAL,
      Type.LESS_THAN,
      Type.LESS_THAN_OR_EQUAL
  );

  private final boolean isWindowed;
  private final ExpressionEvaluator compiledWhereClause;
  private final boolean addAdditionalColumnsToIntermediateSchema;
  private final LogicalSchema intermediateSchema;
  private final MetaStore metaStore;
  private final KsqlConfig ksqlConfig;
  private final LogicalSchema schema = getSource().getSchema();

  // The rewritten predicate in DNF, e.g. (A AND B) OR (C AND D)
  private final Expression rewrittenPredicate;
  // The separated disjuncts.  In the above example, [(A AND B), (C AND D)]
  private final List<Expression> disjuncts;
  private final ImmutableList<LookupConstraint> lookupConstraints;
  private final Set<UnqualifiedColumnReferenceExp> keyColumns = new HashSet<>();
  private final Set<UnqualifiedColumnReferenceExp> systemColumns = new HashSet<>();
  private final QueryPlannerOptions queryPlannerOptions;
  private final boolean requiresTableScan;

  public QueryFilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final boolean isWindowed,
      final QueryPlannerOptions queryPlannerOptions
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    Objects.requireNonNull(predicate, "predicate");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.queryPlannerOptions = queryPlannerOptions;
    // The predicate is rewritten as DNF.  Discussion for why this format is chosen and how it helps
    // to extract keys in various scenarios can be found here:
    // https://github.com/confluentinc/ksql/pull/6874
    this.rewrittenPredicate = PullQueryRewriter.rewrite(predicate);
    this.disjuncts = LogicRewriter.extractDisjuncts(rewrittenPredicate);
    this.isWindowed = isWindowed;

    // Basic validation of WHERE clause
    this.requiresTableScan = validateWhereClauseAndCheckTableScan();

    // Extraction of key and system columns
    extractKeysAndSystemCols();

    // Extraction of lookup constraints
    lookupConstraints = extractLookupConstraints();

    // Compiling expression into byte code/interpreting the expression
    this.addAdditionalColumnsToIntermediateSchema = shouldAddAdditionalColumnsInSchema();
    this.intermediateSchema = QueryLogicalPlanUtil.buildIntermediateSchema(
        source.getSchema().withoutPseudoAndKeyColsInValue(),
        addAdditionalColumnsToIntermediateSchema, isWindowed);
    compiledWhereClause = getExpressionEvaluator(
        rewrittenPredicate, intermediateSchema, metaStore, ksqlConfig, queryPlannerOptions);
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

  public ExpressionEvaluator getCompiledWhereClause() {
    return compiledWhereClause;
  }

  public boolean isWindowed() {
    return isWindowed;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "lookupConstraints is ImmutableList")
  public List<LookupConstraint> getLookupConstraints() {
    return lookupConstraints;
  }

  public boolean getAddAdditionalColumnsToIntermediateSchema() {
    return addAdditionalColumnsToIntermediateSchema;
  }

  public LogicalSchema getIntermediateSchema() {
    return intermediateSchema;
  }

  private boolean validateWhereClauseAndCheckTableScan() {
    for (Expression disjunct : disjuncts) {
      final Validator validator = new Validator();
      validator.process(disjunct, null);
      if (validator.requiresTableScan) {
        return true;
      }
      if (!validator.isKeyedQuery) {
        if (queryPlannerOptions.getTableScansEnabled()) {
          return true;
        } else {
          throw invalidWhereClauseException("WHERE clause missing key column for disjunct: "
              + disjunct.toString(), isWindowed);
        }
      }

      if (!validator.seenKeys.isEmpty()
          && validator.seenKeys.cardinality() != schema.key().size()) {
        if (queryPlannerOptions.getTableScansEnabled()) {
          return true;
        } else {
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
    }
    return false;
  }

  private void extractKeysAndSystemCols() {
    new KeyAndSystemColsExtractor().process(rewrittenPredicate, null);
  }

  /**
   * The WHERE clause is in DNF and this method extracts key constraints from each disjunct.
   * In order to do that successfully, a given disjunct must have equality conditions on the keys.
   * For example, for "KEY = 1 AND WINDOWSTART > 0 OR COUNT > 5 AND WINDOWEND < 10", the disjunct
   * "KEY = 1 AND WINDOWSTART > 0" has a key equality constraint for value 1. The second
   * disjunct "COUNT > 5 AND WINDOWEND < 10" does not and so has an unbound key constraint.
   * seenKeys is used to make sure that all columns of a multi-column
   * key are constrained via an equality condition.
   * keyContents has the key values for each columns of a key.
   * @return the constraints on the key values used to to do keyed lookup.
   */
  private ImmutableList<LookupConstraint> extractLookupConstraints() {
    if (requiresTableScan) {
      LOG.debug("Skipping extracting key value extraction. Already requires table scan");
      return ImmutableList.of(new NonKeyConstraint());
    }
    final ImmutableList.Builder<LookupConstraint> constraintPerDisjunct = ImmutableList.builder();
    for (Expression disjunct : disjuncts) {
      final KeyValueExtractor keyValueExtractor = new KeyValueExtractor();
      keyValueExtractor.process(disjunct, null);

      // Validation and extractions of window bounds
      final Optional<WindowBounds> optionalWindowBounds;
      if (isWindowed) {
        final WindowBounds windowBounds = new WindowBounds();
        new WindowBoundsExtractor().process(disjunct, windowBounds);
        optionalWindowBounds = Optional.of(windowBounds);
      } else {
        optionalWindowBounds = Optional.empty();
      }

      final LookupConstraint constraint =
          keyValueExtractor.getLookupConstraint(optionalWindowBounds);
      constraintPerDisjunct.add(constraint);

    }
    return constraintPerDisjunct.build();
  }

  /**
   * Validate the WHERE clause for pull queries. Each of these validation steps are taken for each
   * disjunct of a DNF expression.
   * 1. There must be exactly one equality condition per key.
   * 2. An IN predicate has been transformed to equality conditions and therefore isn't handled.
   * 3. Only AND is allowed.
   * 4. If there is a multi-key, conditions on all keys must be specified.
   */
  private final class Validator extends TraversalExpressionVisitor<Object> {

    private final BitSet seenKeys;
    private boolean isKeyedQuery;
    private boolean requiresTableScan;

    Validator() {
      isKeyedQuery = false;
      seenKeys = new BitSet(schema.key().size());
      requiresTableScan = false;
    }

    @Override
    public Void process(final Expression node, final Object context) {
      if (!(node instanceof LogicalBinaryExpression)
          && !(node instanceof ComparisonExpression)
              && !(node instanceof LikePredicate)) {
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
        setTableScanOrElseThrow(() ->
            invalidWhereClauseException("Only AND expressions are supported: " + node, false));
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
      // First see if we can find a direct column reference
      final UnqualifiedColumnReferenceExp column = getColumnRefSideOrNull(node);
      if (column != null) {
        final Expression other = getNonColumnRefSide(node);
        final HasColumnRef hasColumnRef = new HasColumnRef();
        hasColumnRef.process(other, null);
        if (hasColumnRef.hasColumnRef()) {
          setTableScanOrElseThrow(() ->
              invalidWhereClauseException("A comparison must be between a key column and a "
                  + "resolvable expression", isWindowed));
          return null;
        }
      } else {
        setTableScanOrElseThrow(() ->
            invalidWhereClauseException("A comparison must directly reference a key column",
                isWindowed));
        return null;
      }
      return visitColumnComparisonExpression(column, node);
    }

    //Check the referred column of the expression
    private Void visitColumnComparisonExpression(
            final UnqualifiedColumnReferenceExp column,
            final ComparisonExpression node
    ) {
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
                        "Bound on non-existent column " + columnName, isWindowed));

        if (col.namespace() == Namespace.KEY) {
          if (!isKeyQuery(node)) {
            setTableScanOrElseThrow(() ->
                invalidWhereClauseException("Bound on key columns '"
                    + getSource().getSchema().key() + "' must currently be '='", isWindowed));
          }
          if (seenKeys.get(col.index()) && !queryPlannerOptions.getTableScansEnabled()) {
            throw invalidWhereClauseException(
                    "A comparison condition on the key column cannot be combined with other"
                            + " comparisons such as an IN predicate", isWindowed);
          }
          seenKeys.set(col.index());
          isKeyedQuery = true;
          return null;
        }
        return null;
      }
    }

    @Override
    public Void visitLikePredicate(final LikePredicate node, final Object context) {
      if (node.getValue() instanceof UnqualifiedColumnReferenceExp) {
        final UnqualifiedColumnReferenceExp column =
                (UnqualifiedColumnReferenceExp) node.getValue();
        final ColumnName columnName = column.getColumnName();
        final Column col = schema.findColumn(columnName)
                .orElseThrow(() -> invalidWhereClauseException(
                        "Like condition on non-existent column " + columnName, isWindowed));
        if (SqlBaseType.STRING != col.type().baseType()) {
          throw invalidWhereClauseException("The column type for Like "
                  + "condition must be VARCHAR. The column type is "
                  + col.type().baseType().toString(), isWindowed);
        }
        final Expression pattern = node.getPattern();
        if (!(pattern instanceof StringLiteral || pattern instanceof NullLiteral)) {
          throw invalidWhereClauseException(
                  "Like condition on non-string pattern " + pattern.getClass().getName(),
                  isWindowed);
        }
      } else {
        setTableScanOrElseThrow(() -> invalidWhereClauseException("Like condition must be between "
                + "strings", isWindowed));
      }
      return null;
    }

    private boolean isKeyQuery(final ComparisonExpression node) {
      if (node.getType() == Type.NOT_EQUAL || node.getType() == Type.IS_DISTINCT_FROM
          || node.getType() == Type.IS_NOT_DISTINCT_FROM) {
        return false;
      }
      return true;
    }

    private void setTableScanOrElseThrow(final Supplier<KsqlException> exceptionSupplier) {
      if (queryPlannerOptions.getTableScansEnabled()) {
        requiresTableScan = true;
      } else {
        throw exceptionSupplier.get();
      }
    }
  }

  private static final class HasColumnRef extends TraversalExpressionVisitor<Object> {

    private boolean hasColumnRef;

    HasColumnRef() {
      hasColumnRef = false;
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Object context
    ) {
      hasColumnRef = true;
      return null;
    }

    public boolean hasColumnRef() {
      return hasColumnRef;
    }
  }

  private UnqualifiedColumnReferenceExp getColumnRefSideOrNull(final ComparisonExpression comp) {
    return (UnqualifiedColumnReferenceExp)
        (comp.getRight() instanceof UnqualifiedColumnReferenceExp
            ? comp.getRight()
            : (comp.getLeft() instanceof UnqualifiedColumnReferenceExp ? comp.getLeft() : null));
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
    private final BitSet seenKeys;
    private final Object[] keyContents;
    private HashMap<Integer, ImmutablePair<Type, SqlType>> operators;

    KeyValueExtractor() {
      keyContents = new Object[schema.key().size()];
      seenKeys = new BitSet(schema.key().size());
      operators = new HashMap<>();
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node, final Object context) {
      final UnqualifiedColumnReferenceExp column = getColumnRefSideOrNull(node);
      final Expression other = getNonColumnRefSide(node);
      Preconditions.checkNotNull(column, "UnqualifiedColumnReferenceExp should be found");
      final ColumnName columnName = column.getColumnName();

      final Optional<Column> col = schema.findColumn(columnName);
      if (col.isPresent() && col.get().namespace() == Namespace.KEY) {
        final Object key = resolveKey(other, col.get(), metaStore, ksqlConfig, node);
        setMostSelectiveConstraint(col.get(), node, key);
      }
      return null;
    }

    public LookupConstraint getLookupConstraint(final Optional<WindowBounds> windowBounds) {
      if (seenKeys.isEmpty()) {
        return new NonKeyConstraint();
      }

      if (operators.size() > 1) {
        //if disjunct consist of multiple operations
        //we set the ground for a keylookup if all ops are of type EQUAL,
        //otherwise we return nokeyconsraint which leads to a table scan
        if (operators.values().stream().allMatch(op -> op.getKey().equals(Type.EQUAL))) {
          return new KeyConstraint(ConstraintOperator.EQUAL,
            GenericKey.fromArray(keyContents), windowBounds);
        }
        return new NonKeyConstraint();
      }

      //single operator disjunct case
      final Type operatorType = operators.get(0).getLeft();
      if (operatorType == Type.EQUAL) {
        return new KeyConstraint(ConstraintOperator.EQUAL,
          GenericKey.fromArray(keyContents), windowBounds);
      } else if (isSupportedType(operators.get(0).getRight())) {
        if (operatorType == Type.GREATER_THAN) {
          return new KeyConstraint(ConstraintOperator.GREATER_THAN,
            GenericKey.fromArray(keyContents), windowBounds);
        } else if (operatorType == Type.GREATER_THAN_OR_EQUAL) {
          return new KeyConstraint(ConstraintOperator.GREATER_THAN_OR_EQUAL,
            GenericKey.fromArray(keyContents), windowBounds);
        } else if (operatorType == Type.LESS_THAN) {
          return new KeyConstraint(ConstraintOperator.LESS_THAN,
            GenericKey.fromArray(keyContents), windowBounds);
        } else if (operatorType == Type.LESS_THAN_OR_EQUAL) {
          return new KeyConstraint(ConstraintOperator.LESS_THAN_OR_EQUAL,
            GenericKey.fromArray(keyContents), windowBounds);
        }
      }

      return new NonKeyConstraint();
    }

    private boolean isSupportedType(final SqlType sqlType) {
      if (sqlType == SqlTypes.STRING) {
        return true;
      } else if (sqlType == SqlTypes.BYTES) {
        return true;
      }
      return false;
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
            "pull query",
            queryPlannerOptions.getInterpreterEnabled()
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

    //Pick the most selective constraint from the predicates
    private void setMostSelectiveConstraint(
        final Column col,
        final ComparisonExpression node,
        final Object key) {
      final int index = col.index();
      if (operators.containsKey(index) && operators.get(index).getLeft() == Type.EQUAL) {
        return;
      }
      if (node.getType() == Type.EQUAL) {
        setConstraint(index, col, node, key);
        return;
      }
      if (!operators.containsKey(index)) {
        setConstraint(index, col, node, key);
      }
    }

    private void setConstraint(
        final int index,
        final Column col,
        final ComparisonExpression node,
        final Object key
    ) {
      keyContents[index] = key;
      seenKeys.set(index);
      operators.put(index, new ImmutablePair<>(node.getType(), col.type()));
    }
  }


  /**
   * Extracts the upper and lower bounds on windowstart/windowend columns.
   * Performs the following validations on the window bounds:
   * 1. An equality bound cannot be combined with other bounds.
   * 2. No duplicate bounds are allowed, such as multiple greater than bounds.
   */
  private final class WindowBoundsExtractor
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
        final Range<Instant> instant = Range.singleton(asInstant(getNonColumnRefSide(node),
            column.getColumnName()));
        result = windowBounds.setEquality(column, instant);
      }
      final Type type = getSimplifiedBoundType(node);

      if (type.equals(Type.LESS_THAN)) {
        final Instant upper = asInstant(getNonColumnRefSide(node), column.getColumnName());
        final BoundType upperType = getRangeBoundType(node);
        result = windowBounds.setUpper(column, Range.upTo(upper, upperType));
      } else if (type.equals(Type.GREATER_THAN)) {
        final Instant lower = asInstant(getNonColumnRefSide(node), column.getColumnName());
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

    private Instant asInstant(final Expression other, final ColumnName name) {
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

      try {
        final Long value = (Long) new GenericExpressionResolver(
            SqlTypes.BIGINT,
            name,
            metaStore,
            ksqlConfig,
            "pull query window bounds extractor",
            queryPlannerOptions.getInterpreterEnabled()
        ).resolve(other);

        return Instant.ofEpochMilli(value);
      } catch (final KsqlException e) {
        throw invalidWhereClauseException(
            "Window bounds must resolve to an INT, BIGINT, or STRING containing a datetime.",
            true
        );
      }
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
            + " - includes a key equality expression, "
            + "e.g. `SELECT * FROM X WHERE <key-column> = Y;`."
            + System.lineSeparator()
            + " - in the case of a multi-column key, is a conjunction of equality expressions "
            + "that cover all key columns."
            + System.lineSeparator()
            + " - to support range expressions, e.g.,  SELECT * FROM X WHERE <key-column> < Y;`, "
            + "range scans need to be enabled by setting ksql.query.pull.range.scan.enabled=true"
            + additional
            + System.lineSeparator()
            + "If more flexible queries are needed, , table scans can be enabled by "
            + "setting ksql.query.pull.table.scan.enabled=true."
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

  private static ExpressionEvaluator getExpressionEvaluator(
      final Expression expression,
      final LogicalSchema schema,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final QueryPlannerOptions queryPlannerOptions) {

    if (queryPlannerOptions.getInterpreterEnabled()) {
      return InterpretedExpressionFactory.create(
          expression,
          schema,
          metaStore,
          ksqlConfig
      );
    } else {
      return CodeGenRunner.compileExpression(
          expression,
          "Predicate",
          schema,
          ksqlConfig,
          metaStore
      );
    }
  }
}
