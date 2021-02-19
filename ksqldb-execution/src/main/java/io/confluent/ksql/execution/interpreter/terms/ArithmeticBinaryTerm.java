package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.DoubleTerm;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.TypedTerm;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class ArithmeticBinaryTerm implements Term {

  private final Term left;
  private final Term right;
  private final ArithmeticBinaryFunction arithmeticBinaryFunction;
  private final SqlType resultType;

  public ArithmeticBinaryTerm(
      final Term left,
      final Term right,
      final ArithmeticBinaryFunction arithmeticBinaryFunction,
      final SqlType resultType
  ) {
    this.left = left;
    this.right = right;
    this.arithmeticBinaryFunction = arithmeticBinaryFunction;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return arithmeticBinaryFunction.doFunction(left.getValue(context), right.getValue(context));
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }

  public interface ArithmeticBinaryFunction {
    Object doFunction(Object o1, Object o2);
  }
}
