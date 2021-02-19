package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;

public class LogicalBinaryTerms {

  public static class AndTerm implements BooleanTerm {
    private final BooleanTerm left;
    private final BooleanTerm right;

    public AndTerm(final BooleanTerm left, final BooleanTerm right) {
      this.left = left;
      this.right = right;
    }

    public Boolean getBoolean(final TermEvaluationContext context) {
      return left.getBoolean(context) && right.getBoolean(context);
    }

    public Object getValue(final TermEvaluationContext context) {
      return getBoolean(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static class OrTerm implements BooleanTerm {
    private final BooleanTerm left;
    private final BooleanTerm right;

    public OrTerm(final BooleanTerm left, final BooleanTerm right) {
      this.left = left;
      this.right = right;
    }

    public Boolean getBoolean(final TermEvaluationContext context) {
      return left.getBoolean(context) || right.getBoolean(context);
    }

    public Object getValue(final TermEvaluationContext context) {
      return getBoolean(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static BooleanTerm create(
      final LogicalBinaryExpression.Type type,
      final BooleanTerm left,
      final BooleanTerm right
  ) {
    switch (type) {
      case OR:
        return new OrTerm(left, right);
      case AND:
        return new AndTerm(left, right);
      default:
        throw new KsqlException("Unknown type " + type);
    }
  }
}
