package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionCallTerm implements Term {

  private final Kudf kudf;
  private final List<Term> arguments;
  private final Class<?> resultJavaClass;
  private final SqlType resultType;

  public FunctionCallTerm(
      final Kudf kudf,
      final List<Term> arguments,
      final Class<?> resultJavaClass,
      final SqlType resultType
  ) {
    this.kudf = kudf;
    this.arguments = arguments;
    this.resultJavaClass = resultJavaClass;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    List<Object> argObjects = arguments.stream()
        .map(term -> term.getValue(context))
        .collect(Collectors.toList());
    final Object result = kudf.evaluate(argObjects.toArray());
    return resultJavaClass.cast(result);
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
