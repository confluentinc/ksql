package io.confluent.ksql.analyzer;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.util.ComparisonUtil;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Validates types used in the where statement.
 */
public final class WhereTypeValidator {

  private final SourceSchemas sourceSchemas;
  private final FunctionRegistry functionRegistry;

  WhereTypeValidator(final SourceSchemas sourceSchemas,
      final FunctionRegistry functionRegistry) {
    this.sourceSchemas = requireNonNull(sourceSchemas, "sourceSchemas");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  /**
   * Validates the given where expression.
   */
  void validateWhereExpression(final Expression exp) {
    // First, collect all of the types so that we can collect one large logical schema.
    final TypeExtractor extractor = new TypeExtractor();
    extractor.process(exp, null);

    // Collect all of the columns together so they can be used by the ExpressionTypeManager.
    LogicalSchema.Builder completeSchema = LogicalSchema.builder();
    for (LogicalSchema schema : extractor.referencedSchemas.values()) {
      completeSchema.keyColumns(schema.key());
      completeSchema.valueColumns(schema.value());
    }

    // Construct an expression type manager to be used when checking types.
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(completeSchema.build(),
        functionRegistry);

    // Traverse the AST and throw errors if necessaryl
    TypeChecker typeChecker = new TypeChecker(expressionTypeManager);
    typeChecker.process(exp, null);
  }

  // Extracts all of the types to be used when evaluating expressions.
  private final class TypeExtractor extends TraversalExpressionVisitor<Object> {

    private final Map<String, LogicalSchema> referencedSchemas = new HashMap<>();

    private TypeExtractor() {
    }

    @Override
    public Void visitColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Object context
    ) {
      final ColumnName reference = node.getColumnName();
      getSchema(Optional.empty(), reference)
          .ifPresent(pair -> referencedSchemas.put(pair.left.text(), pair.right));
      return null;
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Object context
    ) {
      getSchema(Optional.of(node.getQualifier()), node.getColumnName())
          .ifPresent(pair -> referencedSchemas.put(pair.left.text(), pair.right));
      return null;
    }

    private Optional<Pair<SourceName, LogicalSchema>> getSchema(
        final Optional<SourceName> sourceName,
        final ColumnName name
    ) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(sourceName, name);
      if (sourcesWithField.size() != 1) {
        throw new KsqlException("In WHERE clause, column '"
            + sourceName.map(n -> n.text() + KsqlConstants.DOT + name.text())
            .orElse(name.text())
            + "' cannot be resolved or is ambiguous.");
      }

      SourceName foundSourceName = sourcesWithField.iterator().next();
      return sourceSchemas.schemaWithName(foundSourceName)
        .map(schema -> Pair.of(foundSourceName, schema));
    }
  }

  // Rewrites all names to be unqualified, which is required of the ExpressionTypeManager.
  private static final class ColumnReferenceRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    ColumnReferenceRewriter() {
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

  /**
   * Does the actual type checking, ensuring that the where expression uses the proper types and
   * then giving a decent error message if there's an issue.
   */
  private static final class TypeChecker extends VisitParentExpressionVisitor<Void, Object> {

    private final ExpressionTypeManager expressionTypeManager;
    private final ColumnReferenceRewriter columnReferenceRewriter = new ColumnReferenceRewriter();

    TypeChecker(ExpressionTypeManager expressionTypeManager) {
      this.expressionTypeManager = expressionTypeManager;
    }

    public Void visitExpression(final Expression exp, final Object context) {

      Expression rewrite = ExpressionTreeRewriter.rewriteWith(
          columnReferenceRewriter::process, exp);

      SqlType type = expressionTypeManager.getExpressionSqlType(rewrite);
      if (!SqlTypes.BOOLEAN.equals(type)) {
        throw new KsqlException("Type error in WHERE expression: Should evaluate to "
            + "boolean but is " + type.toString(FormatOptions.none()) + " instead.");
      }
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression exp,
        final Object context
    ) {
      Expression leftRewrite = ExpressionTreeRewriter.rewriteWith(
          columnReferenceRewriter::process, exp.getLeft());
      Expression rightRewrite = ExpressionTreeRewriter.rewriteWith(
          columnReferenceRewriter::process, exp.getRight());

      SqlType leftType = expressionTypeManager.getExpressionSqlType(leftRewrite);
      SqlType rightType = expressionTypeManager.getExpressionSqlType(rightRewrite);

      if (!ComparisonUtil.isValidComparison(leftType, exp.getType(), rightType)) {
        throw new KsqlException("Type mismatch in WHERE expression: Cannot compare "
            + exp.getLeft().toString() + " (" + leftType.toString(FormatOptions.none()) + ") to "
            + exp.getRight().toString() + " (" + rightType.toString(FormatOptions.none()) + ").");
      }
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression exp,
        final Object context) {
      process(exp.getLeft(), context);
      process(exp.getRight(), context);
      return null;
    }
  }
}
