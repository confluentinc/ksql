package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class ArithmeticUnaryTerm implements Term {

  private final Term term;
  private final ArithmeticUnaryFunction arithmeticUnaryFunction;

  public ArithmeticUnaryTerm(
      final Term term,
      final ArithmeticUnaryFunction arithmeticUnaryFunction
  ) {
    this.term = term;
    this.arithmeticUnaryFunction = arithmeticUnaryFunction;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return arithmeticUnaryFunction.doFunction(term.getValue(context));
  }

  @Override
  public SqlType getSqlType() {
    return null;
  }

  public interface ArithmeticUnaryFunction {
    Object doFunction(Object o);
  }
}
