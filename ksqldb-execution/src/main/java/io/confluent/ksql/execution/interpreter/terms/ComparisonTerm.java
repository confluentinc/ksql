package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;

public class ComparisonTerm  {

  public static class CompareToTerm implements BooleanTerm {

    private final Term left;
    private final Term right;
    private final ComparisonNullCheckFunction nullCheckFunction;
    private final ComparisonFunction comparisonFunction;
    private final ComparisonCheckFunction comparisonCheckFunction;

    public CompareToTerm(
        final Term left,
        final Term right,
        final ComparisonNullCheckFunction nullCheckFunction,
        final ComparisonFunction comparisonFunction,
        final ComparisonCheckFunction comparisonCheckFunction
    ) {
      this.left = left;
      this.right = right;
      this.nullCheckFunction = nullCheckFunction;
      this.comparisonFunction = comparisonFunction;
      this.comparisonCheckFunction = comparisonCheckFunction;
    }

    @Override
    public Boolean getBoolean(final TermEvaluationContext context) {
      final Optional<Boolean> nullCheck = nullCheckFunction.checkNull(context, left, right);
      if (nullCheck.isPresent()) {
        return nullCheck.get();
      }
      final int compareTo = comparisonFunction.compareTo(context, left, right);
      return comparisonCheckFunction.doCheck(compareTo);
    }

    public Object getValue(final TermEvaluationContext context) {
      return getBoolean(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static class EqualsTerm implements BooleanTerm {

    private final Term left;
    private final Term right;
    private final ComparisonNullCheckFunction nullCheckFunction;
    private final EqualsFunction equalsFunction;
    private final EqualsCheckFunction equalsCheckFunction;

    public EqualsTerm(
        final Term left,
        final Term right,
        final ComparisonNullCheckFunction nullCheckFunction,
        final EqualsFunction equalsFunction,
        final EqualsCheckFunction equalsCheckFunction
    ) {
      this.left = left;
      this.right = right;
      this.nullCheckFunction = nullCheckFunction;
      this.equalsFunction = equalsFunction;
      this.equalsCheckFunction = equalsCheckFunction;
    }

    @Override
    public Boolean getBoolean(final TermEvaluationContext context) {
      final Optional<Boolean> nullCheck = nullCheckFunction.checkNull(context, left, right);
      if (nullCheck.isPresent()) {
        return nullCheck.get();
      }
      final boolean equals = equalsFunction.equals(context, left, right);
      return equalsCheckFunction.doCheck(equals);
    }

    public Object getValue(final TermEvaluationContext context) {
      return getBoolean(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }


  public interface ComparisonFunction {

    int compareTo(TermEvaluationContext context, Term left, Term right);
  }

  public interface ComparisonCheckFunction {

    boolean doCheck(int compareTo);
  }

  public interface EqualsFunction {

    boolean equals(TermEvaluationContext context, Term left, Term right);
  }

  public interface EqualsCheckFunction {
    boolean doCheck(boolean equals);
  }

  public interface ComparisonNullCheckFunction {

    Optional<Boolean> checkNull(TermEvaluationContext context, Term left, Term right);
  }
}
